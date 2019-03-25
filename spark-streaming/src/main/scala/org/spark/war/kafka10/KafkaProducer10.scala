package org.spark.war.kafka10


import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.spark.war.common.{ParamMapUtil, WarContext}
import org.spark.war.log.Logger
import org.spark.war.utils.DataUtil

/**
  * kafka 生产者 1.0.0
  * @author sgr
  * @version 1.0, 2019-03-10 23:38
  **/
class KafkaProducer10 extends Thread {

  private val logger:Logger = Logger.getInstance(classOf[KafkaProducer10])
  private val kafkaConf = WarContext.getMap(KafkaProperties.KAFKA)
  private val broker_list = ParamMapUtil.getStringValue(kafkaConf, KafkaProperties.KAFKA_BROKER_LIST)
  private val topic = ParamMapUtil.getStringValue(kafkaConf, KafkaProperties.KAFKA_TOPIC)
//  private val groupid = ParamMapUtil.getStringValue(kafkaConf, Ka fkaProperties.KAFKA_GROUPID)
//  private val zookeeper_url = ParamMapUtil.getStringValue(kafkaConf, KafkaProperties.KAFKA_ZOOKEEPER_URL)

  private var kafkaProducer:KafkaProducer[String,String] = _

  def getTopic():String = topic

  def getInstance():KafkaProducer[String,String] = {
    this.synchronized {
      if (kafkaProducer == null){
        val properties = new Properties()
        properties.put("bootstrap.servers", broker_list)
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        properties.put("acks", "all") //等待所有副本节点ack应答

        kafkaProducer = new KafkaProducer[String,String](properties)
      }
      kafkaProducer
    }

  }

  override def run(): Unit = {
    var messageNo = 1
    while (true) {
      val message = "message_" + messageNo
      logger.info("KafkaProducer10 send "+message+"...")
      val producer = getInstance()
//      producer.send(new ProducerRecord[String, String](getTopic(), DataUtil.getDataRecord()))

      // 带回调类 可以满足发送成功之后的逻辑
      val data = DataUtil.getDataRecord()
      producer.send(new ProducerRecord[String, String](getTopic(), data),
        new Callback {
          override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
              println("send OK. Record {}" + data)
              println("send OK. Record partition {}" + metadata.partition() + ", offset {}" + metadata.offset())
          }

        }
      )

      messageNo += 1

      Thread.sleep(1000)
    }

  }
}

object KafkaProducer10 {
  def main(args: Array[String]): Unit = {
    new KafkaProducer10().run()
  }
}
