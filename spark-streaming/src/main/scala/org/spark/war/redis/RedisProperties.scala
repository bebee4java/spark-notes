package org.spark.war.redis

/**
  *
  * @author sgr
  * @version 1.0, 2019-03-25 16:12
  **/
object RedisProperties {
  private[redis] val REDIS = "redis"
  private[redis] val REDIS_ADDRESS = "address"
  private[redis] val REDIS_PASSWORD = "password"
  private[redis] val REDIS_MINIDLE = "minidle"
  private[redis] val REDIS_MAXTOTAL = "maxtotal"

  /**
    * 成功码
    */
  private[redis] val SUCC_CODE = 0
  /**
    * 消息超长码
    */
  private[redis] val MSG_TOO_LONG = -5
  /**
    * 发送失败码
    */
  private[redis] val FAIL_CODE = -1

  private[redis] val OFFLINE_CODE = 1
}