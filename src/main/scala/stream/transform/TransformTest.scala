package stream.transform

import org.apache.flink.streaming.api.scala._
import source.SensorReading

/**
  * @author jacky-wangjj
  * @date 2020/8/19
  */
object TransformTest {
  def main(args: Array[String]): Unit = {
    //创建流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val streamFromFile: DataStream[String] = env.readTextFile("D:\\ML\\Code\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    val dataStream: DataStream[SensorReading] = streamFromFile.map(data => {
      val dataArr: Array[String] = data.split(",")
      SensorReading(dataArr(0).trim, dataArr(1).trim.toLong, dataArr(2).trim.toDouble)
    })
    /*
     * 1.基本转换算子、简单聚合算子
     */
    val aggStream: DataStream[SensorReading] = dataStream.keyBy("id") //.keyBy(0)
      //      .sum("temperature") //.sum(2)
      //输出当前传感器最新的温度+10，时间戳是上一次时间戳+1
      .reduce((x, y) => SensorReading(x.id, x.timstemp + 1, y.temperature + 10))

    //打印输出
    aggStream.print("agg result")

    /**
      * 2.多流转换算子 split select
      */
    val splitStream: SplitStream[SensorReading] = dataStream.split(data => {
      if (data.temperature > 30) Seq("high") else Seq("low")
    })
    val highTempStream: DataStream[SensorReading] = splitStream.select("high")
    val lowTempStream: DataStream[SensorReading] = splitStream.select("low")
    val allTempStream: DataStream[SensorReading] = splitStream.select("high", "low")
    highTempStream.print("high")
    lowTempStream.print("low")
    allTempStream.print("all")

    /**
      * 3.合并流
      */
    // Connect 流的数据结构可以不同 只能合并两条流
    // CoMap 可以使用CoMap调整两条流的结构
    val warning: DataStream[(String, Double)] = highTempStream.map(data => (data.id, data.temperature))
    val connectedStream: ConnectedStreams[(String, Double), SensorReading] = warning.connect(lowTempStream)
    val coMapStream: DataStream[Product] = connectedStream.map(
      warningData => (warningData._1, warningData._2, "warning"),
      lowData => (lowData.id, "healthy")
    )
    coMapStream.print("coMap")
    // union 流的数据结构需相同 可以合并多条流
    val unionStream: DataStream[SensorReading] = highTempStream.union(lowTempStream)
    unionStream.print("union")

    //启动任务
    env.execute("transform test")
  }
}
