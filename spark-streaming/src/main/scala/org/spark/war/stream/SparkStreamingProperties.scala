package org.spark.war.stream

/**
  * spark流计算常量类
  * @author sgr
  * @version 1.0, 2019-03-12 19:53
  **/
object SparkStreamingProperties {

  private[stream] val STREAM = "stream"
  private[stream] val PROCESS_DELAY = "process.delay"
  private[stream] val KAFKA_TOPICS = "kafka.topics"
  private[stream] val KAFKA_BROKER_LIST = "kafka.broker.list"
  private[stream] val KAFKA_GROUP_ID = "kafka.groupid"
  private[stream] val DEFAULT = "default"
  private[stream] val KAFKA_OFFSET_MANAGER_CLASS = "kafka.offset.manager.class"
  private[stream] val KAFKA_OFFSET_MANAGER_NAME = "kafka.offset.manager.name"
  private[stream] val KAFKA_OFFSET_MANAGER_DEFAULT_CLASS = "org.spark.war.stream.OffsetManagerDBImpl"


}
