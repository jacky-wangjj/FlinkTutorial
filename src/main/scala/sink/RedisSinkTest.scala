package sink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import source.SensorReading

/**
  * 查看redis结果
  * redis-cli
  * keys *
  * hgetall sensor_temperature
  *
  * @author jacky-wangjj
  * @date 2020/8/24
  */
object RedisSinkTest {
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

    val conf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
      .setPort(6379)
      .build()

    //sink
    dataStream.addSink(new RedisSink(conf, new MyRedisMapper))

    //启动任务
    env.execute("redis sink test")
  }
}

class MyRedisMapper() extends RedisMapper[SensorReading] {
  //定义保存数据到redis的命令
  override def getCommandDescription: RedisCommandDescription = {
    //吧传感器id和温度值保存成哈希表 HSET key field value
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature")
  }

  //定义保存redis的value
  override def getKeyFromData(t: SensorReading): String = {
    t.temperature.toString
  }

  //定义保存到redis的key
  override def getValueFromData(t: SensorReading): String = {
    t.id.toString
  }
}
