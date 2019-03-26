package org.spark.notes

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 窗口操作：每隔3秒，统计前30秒的单词数，数据以3秒一批读取
  *
  * @author sgr
  * @version 1.0, 2019-02-18 23:59
  **/
object NetWorkWCByWindow {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("NetWorkWCByWindow").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val lines = ssc.socketTextStream("localhost", 6789)
    val wc = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKeyAndWindow(
      (a:Int, b:Int) => a+b,
      Seconds(30), //窗口大小
      Seconds(3)) //窗口间隔

    wc.print()

    ssc.start()
    ssc.awaitTermination()
  }


}
