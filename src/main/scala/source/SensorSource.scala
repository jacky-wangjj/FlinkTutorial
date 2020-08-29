package source

import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.collection.immutable
import scala.util.Random

/**
  * @author jacky-wangjj
  * @date 2020/8/19
  */
class SensorSource extends SourceFunction[SensorReading] {
  //定义一个flag，表示数据是否正常运行
  private var running: Boolean = true

  //正常生成数据
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    //初始化一个随机数发生器
    val random: Random = new Random()
    //初始化定义一组传感器温度数据
    var curTemp: immutable.IndexedSeq[(String, Double)] = 1.to(10).map(
      i => ("sensor_" + i, 60 + random.nextGaussian() * 20)
    )
    //循环产生数据
    while (running) {
      //在前一次温度的基础上更新温度值
      curTemp = curTemp.map(
        t => (t._1, t._2 + random.nextGaussian())
      )
      //获取当前时间
      val curTime: Long = System.currentTimeMillis()
      curTemp.foreach(
        t => sourceContext.collect(SensorReading(t._1, curTime, t._2))
      )
      //设置时间间隔
      Thread.sleep(500)
    }
  }

  //取消数据生成
  override def cancel(): Unit = {
    running = false
  }
}
