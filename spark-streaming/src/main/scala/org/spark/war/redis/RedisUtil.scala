package org.spark.war.redis

import java.util

import org.apache.commons.lang3.StringUtils
import org.spark.war.common.{ParamMapUtil, WarContext}
import org.spark.war.log.Logger
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  *
  * @author sgr
  * @version 1.0, 2019-03-25 15:58
  **/
object RedisUtil {
  private val logger:Logger = Logger.getInstance(RedisUtil.getClass)
  private val redisConf = WarContext.getMap(RedisProperties.REDIS)
  private val address = ParamMapUtil.getStringValue(redisConf, RedisProperties.REDIS_ADDRESS)
  private val password = ParamMapUtil.getStringValue(redisConf, RedisProperties.REDIS_PASSWORD)
  private var minIdle = ParamMapUtil.getIntValue(redisConf, RedisProperties.REDIS_MINIDLE)
  private var maxTotal = ParamMapUtil.getIntValue(redisConf, RedisProperties.REDIS_MAXTOTAL)
  private val TIMEOUT = 10000
  private var jedisPool:JedisPool = null

  def init():Unit = {
    this.synchronized {
      if (jedisPool != null) {
        logger.warn("====RedisUtil reconnect jedis...")
        jedisPool.close()
        jedisPool = null
      }
      if (minIdle <= 0) minIdle = 1

      if (maxTotal <= 0) maxTotal = 1

      if (address == null || address.length == 0) {
        throw new Exception("address con't find!!!!")
      }
      val config:JedisPoolConfig = new JedisPoolConfig()
      config.setMinIdle(minIdle)
      config.setMaxTotal(maxTotal)
      config.setTestWhileIdle(true)

      val strs = address.split(":", -1)
      val host = strs(0)
      val port = Integer.parseInt(strs(1))
      var dbIndex = ""
      if (strs.length >= 3 && StringUtils.isNumeric(strs(2))) {
        dbIndex = strs(2)
        jedisPool = new JedisPool(config, host, port, TIMEOUT, password, Integer.valueOf(dbIndex))
      } else {
        jedisPool = new JedisPool(config, host, port, TIMEOUT, password)
      }
    }
  }

  this.synchronized{
    init()
  }

  def close(jedis: Jedis): Unit = {
    try
        if (jedis != null) jedis.close()
    catch {
      case e: Exception =>
        logger.error("returnResource error.", e)
    }
  }
  
  private def getJedis:Jedis = {
    var jedis:Jedis = null
    try {
      jedis = jedisPool.getResource
      if (!jedis.isConnected || !(jedis.ping == "PONG")) {
        close(jedis)
        init()
        jedis = jedisPool.getResource
      }
    } catch {
      case e: Exception =>
        try {
          close(jedis)
          init()
          if (jedisPool != null) jedis = jedisPool.getResource
        } catch {
          case e1: Exception =>
            logger.error("redis 重连失败...，原因：" + e.getMessage)
        }
    }
    jedis
  }

  def setCache(key: String, value: String): Int = {
    var jedis:Jedis = null
    try {
      jedis = getJedis
      jedis.set(key, value)
    } catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        RedisProperties.FAIL_CODE
    } finally {
      close(jedis)
    }
    RedisProperties.SUCC_CODE
  }

  def delCache(key: String): Int = {
    var jedis:Jedis = null
    try {
      jedis = getJedis
      jedis.del(key)
    } catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
         RedisProperties.FAIL_CODE
    } finally {
      close(jedis)
    }
    RedisProperties.SUCC_CODE
  }

  def getSetCache(key: String): util.Set[String] = {
    var jedis: Jedis = null
    try {
      jedis = getJedis
      jedis.smembers(key)
    } catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        null
    } finally {
      close(jedis)
    }
  }

  def addSetCache(key: String, values:String*): Int = {
    var jedis: Jedis = null
    try {
      jedis = getJedis
      if (key != null && !key.isEmpty){
        jedis.sadd(key, values:_*)
      }
      RedisProperties.SUCC_CODE
    } catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        RedisProperties.FAIL_CODE
    } finally {
      close(jedis)
    }
  }

  def dropSetCache(key: String, values: String*): Int = {
    var jedis: Jedis = null
    try {
      jedis = getJedis
      if (key != null && !key.isEmpty)
        jedis.srem(key, values:_*)
      RedisProperties.SUCC_CODE
    } catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        RedisProperties.FAIL_CODE
    } finally {
      close(jedis)
    }
  }

  def delSetCache(key: String): Int = delCache(key)

}
