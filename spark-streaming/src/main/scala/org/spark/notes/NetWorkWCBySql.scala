package org.spark.notes

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
  * sparkstreaming 整合sparksql 进行单词的统计
  *
  * @author sgr
  * @version 1.0, 2019-02-19 00:54
  **/
object NetWorkWCBySql {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("NetWorkWCBySql").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.sparkContext.setLogLevel("WARN") //避免日志太多

    val lines = ssc.socketTextStream("localhost", 6789)
    val words = lines.flatMap(_.split(" "))

    words.foreachRDD({
      (rdd:RDD[String], time:Time) => {
        val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
        import spark.implicits._
        val wordsDataFrame = rdd.map(Record(_)).toDF()
        wordsDataFrame.createOrReplaceTempView("words")
        val wcDF = spark.sql("select word,count(1) count from words group by word")
        println("==========")
        wcDF.show()
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
case class Record(word: String)

object SparkSessionSingleton {
  @transient  private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}
