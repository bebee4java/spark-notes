package org.spark.war.common

import java.io.{File, FileInputStream}
import java.util

import org.spark.war.log.Logger
import org.yaml.snakeyaml.Yaml


/**
  * war项目配置上下文，在这里你可以拿到所有conf
  *
  * @author sgr
  * @version 1.0, 2019-03-03 02:02
  **/
object WarContext {
  val logger:Logger = Logger.getInstance(WarContext.getClass)

  private val yaml = new Yaml()
  private val paramMap:util.HashMap[String,Object] = new util.HashMap[String,Object]()

  logger.info("load spark-war yaml conf start...")
  val confPath = CommonUtil.getConfDir()
  val file = new File(confPath)
  if (file.isDirectory) {
    for (f <- file.listFiles()) {
      if (f.isFile && f.getName.endsWith(".yaml")) {
        paramMap.putAll(loadConfFile(f))
      }
    }
  } else {
    logger.error("Con't find conf path {}", confPath)
  }
  logger.info("load spark-war yaml conf end...")

  def loadConfFile(file: File):util.HashMap[String,Object] = {
    val stream = new FileInputStream(file)
    val paramMap = yaml.load(stream).asInstanceOf[util.HashMap[String, Object]]
    paramMap
  }



  def getParamMap():util.HashMap[String, Object] = paramMap

  def getStringValue(key:String, _default:String):String = ParamMapUtil.getStringValue(paramMap, key, _default)

  def getStringValue(key:String):String = ParamMapUtil.getStringValue(paramMap, key)

  def getMapList(key:String):List[util.HashMap[String, Object]] = ParamMapUtil.getMapList(paramMap, key)

  def getMap(key:String):util.HashMap[String,Object] = ParamMapUtil.getMap(paramMap, key)

  def getLongValue(key:String, _default:Long):Long = ParamMapUtil.getLongValue(paramMap, key, _default)

  def getLongValue(key:String):Long = ParamMapUtil.getLongValue(paramMap, key)


}
