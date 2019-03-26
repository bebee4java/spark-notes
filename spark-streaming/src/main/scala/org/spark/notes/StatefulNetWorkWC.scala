package org.spark.notes

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 有状态的累计接收socket流数据单词统计
  *
  */
object StatefulNetWorkWC {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("StatefulNetWorkWC").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // 使用了stateful 的算子 必须进行checkpoint，生产上建议放在hdfs上
    ssc.checkpoint(".")

    val lines = ssc.socketTextStream("localhost",6789)

    val wc = lines.flatMap(_.split(" ")).map((_, 1))
    val state = wc.updateStateByKey(updateFunction _ )

    state.print()

    ssc.start()
    ssc.awaitTermination()


  }

  def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    var newCount =  runningCount.getOrElse(0)
    for (value <- newValues) {
      newCount += value
    }
    Some(newCount)
  }
}
