package batch

import org.apache.flink.api.scala._

object BatchWordCount {
  def main(args: Array[String]): Unit = {
    //创建一个批处理的执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //文件中读取数据
    val inputDataSet: DataSet[String] = env.readTextFile("D:\\data\\wordcount.txt")
    val wcDataSet: AggregateDataSet[(String, Int)] = inputDataSet.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
    //打印输出
    wcDataSet.print()
  }
}
