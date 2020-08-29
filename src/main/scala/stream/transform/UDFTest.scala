package stream.transform

import org.apache.flink.api.common.functions.{FilterFunction, RichMapFunction}
import org.apache.flink.streaming.api.scala._
import source.SensorReading

/**
  * @author jacky-wangjj
  * @date 2020/8/20
  */
object UDFTest {
  def main(args: Array[String]): Unit = {
    //创建流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val streamFromFile: DataStream[String] = env.readTextFile("D:\\ML\\Code\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    val dataStream: DataStream[SensorReading] = streamFromFile.map(data => {
      val dataArr: Array[String] = data.split(",")
      SensorReading(dataArr(0).trim, dataArr(1).trim.toLong, dataArr(2).trim.toDouble)
    })
    //自定义函数类
    //    val filterStream: DataStream[SensorReading] = dataStream.filter(new MyFilter())
    //匿名函数
    val filterStream: DataStream[SensorReading] = dataStream.filter(_.id.startsWith("sensor_1"))

    filterStream.print("MyFilter")

    //富函数
    val myMapperStream: DataStream[String] = dataStream.map(new MyMapper())
    myMapperStream.print("MyMapper")

    //启动任务
    env.execute("udf test")
  }
}

class MyFilter() extends FilterFunction[SensorReading] {
  override def filter(t: SensorReading): Boolean = {
    t.id.startsWith("sensor_1")
  }
}

class MyMapper() extends RichMapFunction[SensorReading, String] {
  override def map(in: SensorReading): String = {
    "flink"
  }
}
