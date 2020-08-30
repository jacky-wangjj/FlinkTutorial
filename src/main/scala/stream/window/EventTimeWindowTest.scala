package stream.window

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import source.SensorReading

/**
  * event time 统计10s内最低温度
  *
  * @author jacky-wangjj
  * @date 2020/8/25
  */
object EventTimeWindowTest {
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
    //设置watermark时间间隔
    env.getConfig.setAutoWatermarkInterval(100L)
    //从文件输入
    val inputStream: DataStream[String] = env.socketTextStream(host, port)
    //transform
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArr: Array[String] = data.split(",")
      SensorReading(dataArr(0).trim, dataArr(1).trim.toLong, dataArr(2).trim.toDouble)
    })
      //      .assignAscendingTimestamps(_.timstamp * 1000) //有序事件
      //      .assignTimestampsAndWatermarks(new PeriodicAssigner())
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(t: SensorReading): Long = t.timstamp * 1000
    }) //处理乱序事件，延迟1s

    //统计10s内的最低温度
    val minTempPerWindowStream: DataStream[(String, Double)] = dataStream.map(data => (data.id, data.temperature))
      .keyBy(_._1)
      //      .timeWindow(Time.seconds(10)) //开时间窗口 滚动窗口
      .timeWindow(Time.seconds(15), Time.seconds(5)) //滑动窗口
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2))) //reduce做增量聚合
    //打印结果
    minTempPerWindowStream.print("min temp")
    dataStream.print("input data")

    //启动任务
    env.execute("event time window test")
  }
}

class PeriodicAssigner() extends AssignerWithPeriodicWatermarks[SensorReading] {
  //延迟时间，一分钟
  val bound = 60000
  //初始值取最小值
  var maxTs = Long.MinValue

  override def getCurrentWatermark: Watermark = new Watermark(maxTs - bound)

  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    maxTs = maxTs.max(t.timstamp * 1000)
    t.timstamp * 1000
  }
}

class PunctuateAssinger extends AssignerWithPunctuatedWatermarks[SensorReading] {
  override def checkAndGetNextWatermark(t: SensorReading, l: Long): Watermark = new Watermark(l)

  override def extractTimestamp(t: SensorReading, l: Long): Long = t.timstamp * 1000
}
