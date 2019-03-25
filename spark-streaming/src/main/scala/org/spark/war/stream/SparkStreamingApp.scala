package org.spark.war.stream

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.spark.war.common.{ParamMapUtil, WarContext}
import org.spark.war.log.Logger
import org.apache.spark.sql.types.StructType
import org.spark.war.jdbc.JdbcUtil
import org.spark.war.redis.RedisUtil

/**
  *
  * @author sgr
  * @version 1.0, 2019-03-12 19:33
  **/
class SparkStreamingApp {
  private val appName = "SparkStreamingApp"
  private val master = "local[2]"
  private var streamingContext:StreamingContext = _
  private val streamingConfs = WarContext.getMap(SparkStreamingProperties.STREAM)
  private val delay:Long = ParamMapUtil.getLongValue(streamingConfs, SparkStreamingProperties.PROCESS_DELAY, 3L)
  private val kafka_topics:String = ParamMapUtil.getStringValue(streamingConfs, SparkStreamingProperties.KAFKA_TOPICS)
  private val kafka_broker_list:String = ParamMapUtil.getStringValue(streamingConfs, SparkStreamingProperties.KAFKA_BROKER_LIST)
  private val kafka_group_id:String = ParamMapUtil.getStringValue(streamingConfs, SparkStreamingProperties.KAFKA_GROUP_ID)



  private def getStreamingContext():StreamingContext  = {
    this.synchronized {
      if (streamingContext == null) {
        val sparkConf = new SparkConf()
        sparkConf.setAppName(appName)
        sparkConf.setMaster(master)
        streamingContext = new StreamingContext(sparkConf, Seconds(delay))
        streamingContext.sparkContext.setLogLevel("WARN")
      }
      streamingContext
    }
  }

  def action(func:(SparkSession, RDD[ConsumerRecord[String,String]]) => Unit) = {
    val ssc = getStreamingContext()
    val topics = kafka_topics.split(",").toSet
    /**
      * 在spark-streaming-kafka-0-10中，建议将enable.auto.commit设为false。
      * 这个配置只是在这个版本生效，enable.auto.commit如果设为true的话，那么意味着offsets会按照
      * auto.commit.interval.ms中所配置的间隔来周期性自动提交到Kafka中。
      * 在Spark Streaming中，将这个选项设置为true的话会使得Spark应用从kafka中读取数据之后就自动提交，而不是数据处理之后提交，
      * 这不是我们想要的。所以为了更好地控制offsets的提交，我们建议将enable.auto.commit设为false。
      */
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafka_broker_list,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> kafka_group_id,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    StreamUtils.actionStream(ssc,kafkaParams,topics,func)
  }

}

object SparkStreamingApp{
  private val logger:Logger = Logger.getInstance(SparkStreamingApp.getClass)
  def apply(): SparkStreamingApp = new SparkStreamingApp()

  def main(args: Array[String]): Unit = {
    SparkStreamingApp().action(user_active_daily)
  }

  val dateFormat = new SimpleDateFormat()
  val format_date:(String, String, String) => String =
    (date:String, formFormat:String, toFormat:String) => {
      dateFormat.applyPattern(formFormat)
      val d:Date = dateFormat.parse(date)
      dateFormat.applyPattern(toFormat)
      dateFormat.format(d)
    }
  import org.apache.spark.sql.functions._
  val my_date_format = udf(format_date)

  def user_active_daily(spark:SparkSession, rdd:RDD[ConsumerRecord[String,String]]): Unit ={
    val schema = StructType.fromDDL("uid int, channel string, time string,params string")
    val rowRDD = rdd.map{
      record =>
        val cols = record.value.split("\\|")
        Row(cols(0).toInt, cols(1), cols(2), cols(3))
    }

    val df = spark.createDataFrame(rowRDD, schema)
    val df1 = df.select(df("uid"), my_date_format(df("time"),lit("yyyy-MM-dd HH:mm:ss"), lit("yyyy-MM-dd")).as("time")).persist()
    import spark.implicits._
    import scala.collection.JavaConverters._
    df1.select("time").distinct().collect().map {
      time =>
        val day = time.getString(0)
        // 之前已经在redis存储的uid set
        val us_redis = RedisUtil.getSetCache(s"SSUV:$day")
        logger.warn(s"redis key{} SSUV:$day")
        // 转成 DF
        val us_before = (us_redis == null || us_redis.isEmpty) match {
          case true => Seq.empty[String].toDF("uid")
          case false => us_redis.asScala.toSeq.toDF("uid")
        }
        us_before.persist()
        // 当前批次的uv
        val us_batch = df1.where(df1("time") === day).select("uid").distinct().persist()
        // 截止当前总的uv
        val us = us_batch.union(us_before).distinct().persist()
        val uv = us.count()
        logger.warn("======this moment, uv is " + uv)

        val sql = s"""REPLACE INTO active_user_daily_stat
                    (date,app_key,report_type,count)
                  VALUES
                    ('$day',
                    'all',
                    1,
                    $uv)
                """
        // 存入mysql
        JdbcUtil.update(sql)
        val us1 =  us_batch.map(_.getInt(0).toString).collect()
        // 将当前批次的uv存入redis
        RedisUtil.addSetCache(s"SSUV:$day", us1:_*)
        df1.unpersist()
        us_before.unpersist()
        us_batch.unpersist()
        us.unpersist()
    }
  }
}
