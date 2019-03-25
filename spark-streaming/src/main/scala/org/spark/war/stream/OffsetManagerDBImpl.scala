package org.spark.war.stream

import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import org.spark.war.common.{ParamMapUtil, WarContext}
import org.spark.war.jdbc.JdbcUtil

import org.spark.war.log.Logger

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
  * kafka offset 管理类 存储DB实现
  * @author sgr
  * @version 1.0, 2019-03-13 16:00
  **/
class OffsetManagerDBImpl extends OffsetManager {
  private val logger:Logger = Logger.getInstance(classOf[OffsetManagerDBImpl])
  private val streamingConfs = WarContext.getMap(SparkStreamingProperties.STREAM)
  private val kafka_group_id:String = ParamMapUtil.getStringValue(streamingConfs, SparkStreamingProperties.KAFKA_GROUP_ID)
  private val kafka_topics:String = ParamMapUtil.getStringValue(streamingConfs, SparkStreamingProperties.KAFKA_TOPICS)
  private val broker_list = ParamMapUtil.getStringValue(streamingConfs, SparkStreamingProperties.KAFKA_BROKER_LIST)



  override def commitOffsets(offsetRanges: Array[OffsetRange]): Boolean = {
    logger.info("==== OffsetManagerDBImpl ==== commitOffsets")
    offsetRanges.foreach { offsetRange =>
      val topic = offsetRange.topic
      val partition = offsetRange.partition
      val fromOff = offsetRange.fromOffset
      val offset = offsetRange.untilOffset
      val sql = s"""REPLACE INTO stream_kafka_offset
                    (topic,_partition,custom_group,_offset)
                  VALUES
                    ('$topic',
                    $partition,
                    '$kafka_group_id',
                    $offset)
                """
      Try {
        logger.info(s"|Topic: $topic\t|Partition: $partition\t|From: $fromOff\t|Until: $offset\t|Sum: ${offset - fromOff} |")
        JdbcUtil.update(sql)
      } match {
        case Success(i) => i
        case Failure(i) =>
          logger.error("update kafka offset error!When exec sql: " + sql)
          return false
      }
    }
    true
  }

  override def retrieveOffsets: Map[TopicPartition, Long] = {
    logger.info("==== OffsetManagerDBImpl ==== retrieveOffsets")
    val dbOffs:Map[TopicPartition,Long] = getDBTopicOffsets
    val kafkaOffs:Map[TopicPartition, Seq[Long]] = getKafaTopicOffsets

    dbOffs.isEmpty match {
      case true => {
        // 还没有消费
        logger.warn("spark streaming app hasn't been apply yet")
        // 取最早的offset进行消费
        kafkaOffs.map{ case (tp, Seq(e, l)) => tp -> e }
      }
      case false => {
        // 消费过
        logger.warn("spark streaming app has been apply")
        // 将kafka的offset 和 自定义存储的进行比较
        (kafkaOffs /: dbOffs) {
          // 将dbOffs 和 kafkaOffs 按key合并
          case (map, (k, v)) => map + (k-> (v +: map.getOrElse(k, Nil)))
        } filter {
          // 过滤出那些 有kafkaOff信息的
          case (_, seq) => seq.size > 1
        } map {
          // 取出合适的offset: kafkaOff(e, l) dbOff(o)
          // 1. o e l 取 e 说明dbOff过期了
          // 2. e l o 取 l 异常情况
          // 3. e o l 取 o 正常情况
          case (tp, o) => tp -> o.sorted.dropRight(1).last
        }

      }
    }
  }

  private def getDBTopicOffsets:Map[TopicPartition,Long] = {
    val topics = kafka_topics.split(",").map(x => "\'"+x+"\'").mkString(",")
    val sql =
      s"""
        select topic, _partition, _offset
        from stream_kafka_offset
        where topic in ($topics) and custom_group = '$kafka_group_id'
      """
    val result = JdbcUtil.query(sql)
    var offs_db = mutable.Map[TopicPartition,Long]()
    result.foreach{
      x =>
        val topic = x.get("topic").asInstanceOf[String]
        val _partition = x.get("_partition").asInstanceOf[Int]
        val _offset = x.get("_offset").asInstanceOf[Long]
        offs_db.+=((new TopicPartition(topic, _partition), _offset))
    }
    logger.warn("get db saved offsets info: " + offs_db)
    offs_db.toMap
  }

  private def getKafaTopicOffsets:Map[TopicPartition, Seq[Long]] = {
    val properties = new Properties()
    properties.put("bootstrap.servers", broker_list)
    properties.put("group.id", kafka_group_id)
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("request.timeout.ms", "15000")
    val consumer = new KafkaConsumer[String, String](properties)
    val result = mutable.Map[TopicPartition,Seq[Long]]()
    import scala.collection.JavaConverters._
    kafka_topics.split(",").foreach{
      topic =>
        val map = consumer.partitionsFor(topic).asScala.map{
          t =>
            val tp = new TopicPartition(t.topic, t.partition)
            consumer.assign(Seq(tp).asJava)
            consumer.seekToEnd(Seq(tp).asJava)
            val lastest_offset = consumer.position(tp)
            consumer.seekToBeginning(Seq(tp).asJava)
            val earlist_offset = consumer.position(tp)
            (tp , Seq(earlist_offset, lastest_offset))
        }
        map.foreach(x => result.+=(x))
    }
    logger.warn("get kafka offsets info: " + result)
    result.toMap
  }
}



object OffsetManagerDBImpl {
  var instant:OffsetManagerDBImpl = null
  def apply():OffsetManagerDBImpl ={
    if(instant == null) instant= new OffsetManagerDBImpl
    instant
  }
}
