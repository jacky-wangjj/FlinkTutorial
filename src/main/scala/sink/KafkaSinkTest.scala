package sink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import source.SensorReading

/**
  * 开启生产者，生产数据，作为source
  * kafka-console-producer.sh --broker-list localhost:9092 --topic sensor
  * 开启消费者，消费数据，作为sink
  * kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sinkTest
  *
  * @author jacky-wangjj
  * @date 2020/8/24
  */
object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    //创建流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //从文件输入
    //    val inputStream: DataStream[String] = env.readTextFile("D:\\ML\\Code\\FlinkTutorial\\src\\main\\resources\\sensor.txt")
    //从kafka进 kafka出
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    val topic: String = "sensor"
    val inputStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), properties))

    val dataStream: DataStream[String] = inputStream.map(data => {
      val dataArr: Array[String] = data.split(",")
      SensorReading(dataArr(0).trim, dataArr(1).trim.toLong, dataArr(2).trim.toDouble)
        .toString() //方便序列化输出
    })

    //sink
    dataStream.addSink(new FlinkKafkaProducer011[String]("localhost:9092", "sinkTest", new SimpleStringSchema()))
    dataStream.print()

    //启动任务
    env.execute("kafka sink test")
  }
}
