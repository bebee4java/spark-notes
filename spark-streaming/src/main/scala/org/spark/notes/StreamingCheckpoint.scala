package org.spark.notes

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
  *
  * @author sgr
  * @version 1.0, 2019-03-26 19:43
  **/
object StreamingCheckpoint {
  def main(args: Array[String]): Unit = {
    val checkpointPath = ""

    val context = StreamingContext.getOrCreate(checkpointPath, () => creatStreamingContext(checkpointPath))


    context.start()
    context.awaitTermination()
  }

  def creatStreamingContext(checkpointPath: String): StreamingContext = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(3))

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val words: DStream[String] = lines.flatMap(_.split(" "))

    val pairs: DStream[(String, Int)] = words.map((_, 1))

    val wordCounts: DStream[(String, Int)] = pairs.reduceByKey(_ + _)

    wordCounts.print()

    ssc.checkpoint(checkpointPath)

    ssc
  }

}
