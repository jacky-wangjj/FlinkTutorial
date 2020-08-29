package stream

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import source.SensorReading

/**
  * @author jacky-wangjj
  * @date 2020/8/25
  */
object SideOutputTest {
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

    val processedStream: DataStream[SensorReading] = dataStream.process(new FreezingAlert())
    //打印结果
    processedStream.print("processed data")
    //打印侧输出流
    processedStream.getSideOutput(new OutputTag[String]("freezing alert")).print("alert data")

    //启动任务
    env.execute("side output test")
  }
}

//冰点报警，如果小时32 C，输出报警信息到侧输出流
class FreezingAlert() extends ProcessFunction[SensorReading, SensorReading] {
  lazy val alertOutput: OutputTag[String] = new OutputTag[String]("freezing alert")

  override def processElement(i: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
    if (i.temperature < 32.0) {
      context.output(alertOutput, "freezing alert for " + i.id)
    } else {
      collector.collect(i)
    }
  }
}
