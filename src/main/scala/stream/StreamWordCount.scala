package stream

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
  * 启动socket生成数据
  * nc -l -p 7777
  */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //获取输入参数--host localhost --port 7777
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")

    //创建流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    //    env.setParallelism(2)
    //设置关闭任务链，任务链可以避免算子间数据传输序列化和反序列化操作，
    // 前后算子在同一个solt执行
    //    env.disableOperatorChaining()
    //接收socket数据流
    val textDataStream: DataStream[String] = env.socketTextStream(host, port)
    //读取数据，分词统计。
    //每一个算子均可设置并行度.setParallelism(1)
    val wordCountDataStream: DataStream[(String, Int)] = textDataStream.flatMap(_.split("\\s"))
      .filter(_.nonEmpty)
      //      .startNewChain()  //开启生成新的任务链，可定制某算子之前不合并任务链，之后合并任务链
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    //打印输出
    wordCountDataStream.print()
    //启动任务
    env.execute("flink stream wordcount")
  }
}
