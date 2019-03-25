package org.spark.war.stream

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.spark.war.log.Logger

import scala.reflect.ClassTag

/**
  *
  * @author sgr
  * @version 1.0, 2019-03-24 16:59
  **/
object StreamUtils {
  private val logger:Logger = Logger.getInstance(StreamUtils.getClass)
  private val offsetManager = OffsetManager.getInstance().get

  def createDStream[K: ClassTag,
  V: ClassTag](ssc: StreamingContext,
  kafkaParams: Map[String, Object],
  topics: Set[String]): InputDStream[ConsumerRecord[K, V]] =  {

    val offsets = offsetManager.retrieveOffsets
    KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[K,V](topics,kafkaParams,offsets))
  }

  def getSession(): SparkSession = {
    // 获取SparkSession
    val spark = SparkSession
      .builder
      .config("spark.debug.maxToStringFields", "100")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.streaming.kafka.maxRatePerPartition", "500")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    spark
  }

  private val spark = getSession()

  def actionStream(ssc: StreamingContext,
                   kafkaParams: Map[String, Object],
                   topics: Set[String],
                   trans_stream:(SparkSession, RDD[ConsumerRecord[String,String]]) => Unit): Unit ={
    val dStream = createDStream[String,String](ssc, kafkaParams, topics)


    dStream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      if (!rdd.isEmpty()) {
        logger.warn("this batch offset{} " + offsetRanges.mkString("\t"))
        trans_stream(spark, rdd)
        offsetManager.commitOffsets(offsetRanges)
      } else {
        logger.warn("this batch no data....")
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }


}
