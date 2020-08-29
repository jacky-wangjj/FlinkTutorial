package stream.window

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import source.SensorReading

/**
  * processing time 统计10s秒内最低温度
  *
  * @author jacky-wangjj
  * @date 2020/8/25
  */
object ProcessingTimeWindowTest {
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

    //统计10s内的最低温度
    val minTempPerWindowStream: DataStream[(String, Double)] = dataStream.map(data => (data.id, data.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(10)) //开时间窗口
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2))) //reduce做增量聚合
    //打印结果
    minTempPerWindowStream.print("min temp")
    dataStream.print("input data")

    //启动任务
    env.execute("processing time window test")
  }
}
