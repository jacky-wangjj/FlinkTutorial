package sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import source.SensorReading

/**
  * 创建mysql结果存储表
  * create table temperatures(sensor varchar(20) not null, temp double not null);
  *
  * @author jacky-wangjj
  * @date 2020/8/24
  */
object JdbcSinkTest {
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

    //sink
    dataStream.addSink(new MyJdbcSink())

    //启动任务
    env.execute("jdbc sink test")
  }
}

class MyJdbcSink() extends RichSinkFunction[SensorReading] {
  //定义sql连接、预编译器
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  //初始化，创建连接和预编译语句
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = DriverManager.getConnection("jdbc:mysql:localhost:3306/test", "root", "123456")
    insertStmt = conn.prepareStatement("INSERT INTO temperatures(sensor, temp) values (?,?)")
    updateStmt = conn.prepareStatement("UPDATE temperatures Set temp = ? WHERE sensor = ?")
  }

  //调用连接，执行sql
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    //执行更新语句
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()
    //如果update没有查找到数据，执行插入语句
    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }
  }

  //释放资源
  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}
