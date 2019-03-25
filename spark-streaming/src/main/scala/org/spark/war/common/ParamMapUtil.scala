package org.spark.war.common

import java.util

/**
  *
  * @author sgr
  * @version 1.0, 2019-03-03 02:54
  **/
object ParamMapUtil {

  def getStringValue(map:util.HashMap[String, Object], key:String, _default:String):String = {
    try {
      val obj = map.get(key)
       if (obj != null) String.valueOf(obj) else _default
    }catch {
      case ex: Exception => _default
    }
  }

  def getStringValue(map:util.HashMap[String, Object], key:String):String = {
    getStringValue(map, key, null)
  }

  def getIntValue(map:util.HashMap[String, Object], key:String, _default:Int):Int = {
    try {
      val obj = map.get(key)
      if (obj != null) obj.asInstanceOf[Number].intValue() else _default
    }catch {
      case ex: Exception => _default
    }
  }

  def getIntValue(map:util.HashMap[String, Object], key:String):Int = {
    getIntValue(map, key, 0)
  }

  def getLongValue(map:util.HashMap[String, Object], key:String, _default:Long):Long = {
    try {
      val obj = map.get(key)
      if (obj != null) obj.asInstanceOf[Number].longValue() else _default
    }catch {
      case ex: Exception => _default
    }
  }

  def getLongValue(map:util.HashMap[String, Object], key:String):Long = {
    getLongValue(map, key, 0L)
  }


  def getMapList(map:util.HashMap[String, Object], key:String):List[util.HashMap[String, Object]] = {
    try {
      val obj = map.get(key)
      if (obj != null) obj.asInstanceOf[List[util.HashMap[String, Object]]] else null
    }catch {
      case ex: Exception => null
    }
  }

  def getMap(map:util.HashMap[String, Object], key:String):util.HashMap[String,Object] = {
    try {
      val obj = map.get(key)
      if (obj != null) obj.asInstanceOf[util.HashMap[String,Object]] else null
    }catch {
      case ex: Exception => null
    }
  }


}
