package org.spark.war.kafka10

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.spark.war.common.{ParamMapUtil, WarContext}
import org.spark.war.log.Logger

/**
  * kafka消费者 1.0.0
  * @author sgr
  * @version 1.0, 2019-03-11 10:42
  **/
class KafkaConsumer10{
  private val logger:Logger = Logger.getInstance(classOf[KafkaConsumer10])
  private val kafkaConf = WarContext.getMap(KafkaProperties.KAFKA)
  private val topic = ParamMapUtil.getStringValue(kafkaConf, KafkaProperties.KAFKA_TOPIC)
  private val broker_list = ParamMapUtil.getStringValue(kafkaConf, KafkaProperties.KAFKA_BROKER_LIST)
  private val group_id = ParamMapUtil.getStringValue(kafkaConf, KafkaProperties.KAFKA_GROUPID)
//  private val zookeeper_url = ParamMapUtil.getStringValue(kafkaConf, KafkaProperties.KAFKA_ZOOKEEPER_URL)

  private var kafkaConsumer:KafkaConsumer[String,String] = _
  def getTopic() = topic

  def getInstance():KafkaConsumer[String, String] = {
    this.synchronized {
      if (kafkaConsumer == null){
        val properties = new Properties()
        properties.put("bootstrap.servers", broker_list)
//        properties.put("zookeeper.connect", zookeeper_url)
        properties.put("group.id", group_id)
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        kafkaConsumer = new KafkaConsumer[String, String](properties)
      }
      kafkaConsumer
    }
  }

  def start(): Unit = {
    val consumer:KafkaConsumer[String, String] = getInstance()
    val mainThread = Thread.currentThread()

       /**
         * 结束关闭资源:
         * 退出循环需要通过另一个线程调用consumer.wakeup()方法
         * 调用consumer.wakeup()可以退出poll(),并抛出WakeupException异常
         * 我们不需要处理 WakeupException,因为它只是用于跳出循环的一种方式
         * consumer.wakeup()是消费者唯一一个可以从其他线程里安全调用的方法
         * 如果循环运行在主线程里，可以在 ShutdownHook里调用该方法
        */
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
      override def run(): Unit = {
        if (consumer != null){
          consumer.wakeup()
          logger.info("wakeup consumer ..........")
          try {
            // 主线程继续执行，以便可以关闭consumer，提交偏移量
            mainThread.join()
          } catch {
            case e:Exception => println(e.getMessage)
          }

        }
      }
    }))

    try {
      while(true){
        consumer.subscribe(util.Arrays.asList(topic))
        val consumerRecords:ConsumerRecords[String, String] = consumer.poll(100)

        val records = consumerRecords.iterator()

        while (records.hasNext){
          val message = records.next().value()
          logger.info(message)
        }

      }
    }finally {
      // 在退出线程之前调用consumer.close()是很有必要的，它会提交任何还没有提交的东西，并向组协调器发送消息，告知自己要离开群组。
      // 接下来就会触发再均衡，而不需要等待会话超时。
      consumer.close()
      logger.info(" close consumer ........")
    }

  }
}

object KafkaConsumer10 {
  def apply(): KafkaConsumer10 = new KafkaConsumer10()
  def main(args: Array[String]): Unit = {
    new KafkaConsumer10().start()
  }
}