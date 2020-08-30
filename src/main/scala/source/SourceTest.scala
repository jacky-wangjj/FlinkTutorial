package source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
  * @author jacky-wangjj
  * @date 2020/8/19
  */
object SourceTest {
  def main(args: Array[String]): Unit = {
    //创建流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //1.从自定义的激活中读取数据
    val stream1: DataStream[SensorReading] = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.80),
      SensorReading("sensor_6", 1547718201, 35.40),
      SensorReading("sensor_7", 1547718202, 32.40),
      SensorReading("sensor_10", 1547718205, 38.40)
    ))
    //打印输出
    stream1.print("stream1").setParallelism(1)
    //2.从文件中读取数据
    val stream2: DataStream[String] = env.readTextFile("D:\\ML\\Code\\FlinkTutorial\\src\\main\\resources\\sensor.txt")
    //打印输出
    stream2.print("stream2").setParallelism(1)
    //3.从kafka中读取数据
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    val topic: String = "sensor"
    val stream3: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), properties))
    //打印输出
    stream3.print("stream3").setParallelism(1)
    //4.自定义source
    val stream4: DataStream[SensorReading] = env.addSource(new SensorSource())
    //打印输出
    stream4.print("stream4").setParallelism(1)
    //启动任务
    env.execute("source test")
  }
}

case class SensorReading(id: String, timstamp: Long, temperature: Double)
