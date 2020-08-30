package stream

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import source.SensorReading

/**
  * 1s内温度连续上升，输出报警信息
  *
  * @author jacky-wangjj
  * @date 2020/8/25
  */
object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    //获取输入参数--host localhost --port 7777
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")
    //创建流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置使用EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //checkpoint设置
    env.enableCheckpointing(60000) //每分钟保存检查点
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(100000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1) //并行checkpoint限制
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(100) //checkpoint最小时间间隔
    //重启策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 500)) //500秒内重启3次
    env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.seconds(300), Time.seconds(10))) //300秒内重启3次，每次重启最少间隔10秒
    //设置StateBackend
    //    env.setStateBackend(new MemoryStateBackend())
    //    env.setStateBackend(new FsStateBackend("file:///tmp/checkpoints"))
    //从文件输入
    val inputStream: DataStream[String] = env.socketTextStream(host, port)
    //transform
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArr: Array[String] = data.split(",")
      SensorReading(dataArr(0).trim, dataArr(1).trim.toLong, dataArr(2).trim.toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(t: SensorReading): Long = t.timstamp * 1000
    }) //处理乱序事件，延迟1s

    val processedStream: DataStream[String] = dataStream.keyBy(_.id)
      .process(new TempIncreAlert())

    dataStream.print("input data")
    processedStream.print("processed data")

    val processedStream2: DataStream[(String, Double, Double)] = dataStream.keyBy(_.id)
      //      .process(new TempChangeAlert(10.0)) //第一种方法，使用process
      //      .flatMap(new TempChangeAlert2(10.0)) //第二种方法，使用flatMap富函数进行状态运算
      .flatMapWithState[(String, Double, Double), Double] {
      //如果没有状态的话，首条数据，直接将当前数据存储状态
      case (input: SensorReading, None) => (List.empty, Some(input.temperature))
      //如果有状态，就应该与上次的温度值比较差值，如果大于阈值输出报警
      case (input: SensorReading, lastTemp: Some[Double]) => {
        val diff: Double = (input.temperature - lastTemp.get).abs
        if (diff > 10.0) {
          (List((input.id, lastTemp.get, input.temperature)), Some(input.temperature))
        } else {
          (List.empty, Some(input.temperature))
        }
      }
    } //第三种方法，使用flatMapWithState

    processedStream2.print("processed2 data")

    //启动任务
    env.execute("process function test")
  }
}

class TempIncreAlert() extends KeyedProcessFunction[String, SensorReading, String] {
  //定义一个状态，用来保存上一个数据的温度值
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double](
    "lastTemp", classOf[Double]
  ))
  //定义一个状态，用来保存定时器的时间戳
  lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long](
    "currentTimer", classOf[Long]
  ))

  override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    //先取出上一个温度值
    val preTemp = lastTemp.value()
    //更新温度值
    lastTemp.update(i.temperature)
    //获取上一个定时器状态的时间戳
    val curTimerTs = currentTimer.value()
    //温度上升，则注册定时器
    if (i.temperature > preTemp && curTimerTs == 0) {
      val timerTs: Long = context.timerService().currentProcessingTime() + 1000L
      context.timerService().registerProcessingTimeTimer(timerTs)
      //更新定时器状态的时间戳
      currentTimer.update(timerTs)
    } else if (preTemp > i.temperature || preTemp == 0.0) {
      //如果温度下降，或是第一条数据，删除定时器并清空状态
      context.timerService().deleteProcessingTimeTimer(curTimerTs)
      currentTimer.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    //输出报警信息
    out.collect(ctx.getCurrentKey + "温度连续上升")
    currentTimer.clear()
  }
}

class TempChangeAlert(threshold: Double) extends KeyedProcessFunction[String, SensorReading, (String, Double, Double)] {
  //定义一个状态变量，保存上次的温度值
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))

  override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#Context, collector: Collector[(String, Double, Double)]): Unit = {
    //获取上次的温度值
    val lastTemp: Double = lastTempState.value()
    //用当前温度值和上次温度值求差，如果大于阈值，输出报警信息
    val diff: Double = (i.temperature - lastTemp).abs
    if (diff > threshold) {
      collector.collect(i.id, lastTemp, i.temperature)
    }
    lastTempState.update(i.temperature)
  }
}

class TempChangeAlert2(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {
  private var lastTempState: ValueState[Double] = _

  //初始化的时候声明state变量
  override def open(parameters: Configuration): Unit = {
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  }

  override def flatMap(in: SensorReading, collector: Collector[(String, Double, Double)]): Unit = {
    //获取上次的温度值
    val lastTemp: Double = lastTempState.value()
    //用当前温度值和上次温度值求差，如果大于阈值，输出报警信息
    val diff: Double = (in.temperature - lastTemp).abs
    if (diff > threshold) {
      collector.collect(in.id, lastTemp, in.temperature)
    }
    lastTempState.update(in.temperature)
  }
}
