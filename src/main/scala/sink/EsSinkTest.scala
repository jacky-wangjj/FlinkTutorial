package sink

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import source.SensorReading

/**
  * 查看elasticsearch索引
  * http://localhost:9200/_cat/indices?v
  * 查看sensor索引下的数据
  * http://localhost:9200/sensor/_search?pretty
  *
  * @author jacky-wangjj
  * @date 2020/8/24
  */
object EsSinkTest {
  def main(args: Array[String]): Unit = {
    //创建流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //从文件输入
    val inputStream: DataStream[String] = env.readTextFile("D:\\ML\\Code\\FlinkTutorial\\src\\main\\resources\\sensor.txt")
    //transform
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArr: Array[String] = data.split(",")
      SensorReading(dataArr(0).trim, dataArr(1).trim.toLong, dataArr(2).trim.toDouble)
    })

    val httpHosts: util.ArrayList[HttpHost] = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost", 9200))

    //创建esSink的Builder
    val esSinkBuilder: ElasticsearchSink.Builder[SensorReading] = new ElasticsearchSink.Builder[SensorReading](
      httpHosts,
      new ElasticsearchSinkFunction[SensorReading] {
        override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          println("saving data" + t)
          //包装成一个map或jsonObject
          val json: util.HashMap[String, String] = new util.HashMap[String, String]()
          json.put("sensor_id", t.id)
          json.put("temperature", t.temperature.toString)
          json.put("ts", t.timstemp.toString)

          //创建index request,准备发送数据
          val indexRequest: IndexRequest = Requests.indexRequest()
            .index("sensor")
            .`type`("readingdata")
            .source(json)

          //利用index发送请求，写入数据
          requestIndexer.add(indexRequest)
          println("data saved.")
        }
      }
    )
    //sink
    dataStream.addSink(esSinkBuilder.build())

    //启动任务
    env.execute("es sink test")
  }
}
