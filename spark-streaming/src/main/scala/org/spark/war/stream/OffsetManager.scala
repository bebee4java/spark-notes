package org.spark.war.stream

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import org.spark.war.common.{ParamMapUtil, WarContext}
import org.spark.war.log.Logger


/**
  * kafka offset 管理类
  * @author sgr
  * @version 1.0, 2019-03-13 15:39
  **/
private[stream] abstract class OffsetManager {
  /**
    * 提交每个分区对呀的offset
    */
  def commitOffsets(offsetRanges:Array[OffsetRange]):Boolean

  /**
    * 恢复offset,返回每个partition的offset
    */
  def retrieveOffsets:Map[TopicPartition,Long]

}
object OffsetManager {
  private val logger:Logger = Logger.getInstance(classOf[OffsetManager])
  private var offsetManagerMap:Map[String, OffsetManager] = Map()
  private val streamingConfs = WarContext.getMap(SparkStreamingProperties.STREAM)

  def getInstance():Option[OffsetManager] = {
    getInstance(SparkStreamingProperties.DEFAULT)
  }

  def getInstance(name:String):Option[OffsetManager] = {
    this.synchronized {
      var offsetManager = offsetManagerMap.get(name)
      if (offsetManager.isEmpty){
        val manager_name = ParamMapUtil.getStringValue(streamingConfs, SparkStreamingProperties.KAFKA_OFFSET_MANAGER_NAME,
          SparkStreamingProperties.DEFAULT)
        if (manager_name.equals(name)){
          val clazz = ParamMapUtil.getStringValue(streamingConfs,
            SparkStreamingProperties.KAFKA_OFFSET_MANAGER_CLASS,
            SparkStreamingProperties.KAFKA_OFFSET_MANAGER_DEFAULT_CLASS
          )
          val manager:OffsetManager = Class.forName(clazz).newInstance().asInstanceOf[OffsetManager]
          offsetManagerMap += (manager_name -> manager)
          offsetManager = Option(manager)
        } else {
          logger.error("can't get the OffsetManager instance by name{}" + manager_name)
        }
      }

      offsetManager
    }
  }
}
