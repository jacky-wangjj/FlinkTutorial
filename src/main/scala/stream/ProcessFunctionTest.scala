package stream

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
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
    //从文件输入
    val inputStream: DataStream[String] = env.socketTextStream(host, port)
    //transform
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArr: Array[String] = data.split(",")
      SensorReading(dataArr(0).trim, dataArr(1).trim.toLong, dataArr(2).trim.toDouble)
    })

    val processedStream: DataStream[String] = dataStream.keyBy(_.id)
      .process(new TempIncreAlert())

    dataStream.print("input data")
    processedStream.print("processed data")

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
