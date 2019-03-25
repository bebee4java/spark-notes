package org.spark.war.utils

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.google.gson.Gson

import scala.util.Random

/**
  * 数据产生工具类
  * @author sgr
  * @version 1.0, 2019-03-11 00:54
  **/
object DataUtil {

  private val random = new Random()
  private val dateFormat = new SimpleDateFormat("yyyy-MM-dd H:mm:ss")

  private val users = Array("1001", "1002","1003", "1004","1005", "1006",
    "2001", "10201","10010", "12001","41001", "108801","109801", "331001","1000101",
    "3001", "10301","10011", "13001","51001", "109901","108701", "441001","1000201",
    "4001", "10401","10012", "14001","61001", "105501","107601", "551001","1000301",
    "5001", "10501","10013", "15001","71001", "106601","106501", "661001","1000401",
    "6001", "10601","10014", "16001","81001", "107701","105401", "771001","1000501",
    "7001", "10701","10015", "17001","91001", "103301","104301", "881001","1000601",
    "8001", "10801","10016", "18001","101001", "102201","103201", "991001","1000701",
    "9001", "10901","10017", "19001","131001", "101101","102101", "111001","1000801",
    "10001", "10011","10018", "11001","141001", "100001","101101", "221001","1000901"
  )
  private val usersCnt = users.length

  private val channels = Array("android", "ios", "web", "other")

  private val channelsCnt = channels.length

  private val actions = Array("click", "browse", "collect", "buy")

  private val actionsCnt = actions.length

  private val categorys = Array("mobile", "book", "clothes", "macbook")

  private val categorysCnt = categorys.length

  def getTime():String = {
      dateFormat.format(new Date())
  }

  def getDataRecord(): String ={

    val user = users(random.nextInt(usersCnt))
    val channel = channels(random.nextInt(channelsCnt))
    val action = actions(random.nextInt(actionsCnt))
    val category = categorys(random.nextInt(categorysCnt))

    val time = getTime()
    val event = new util.HashMap[String,Any]()
    event.put("action", action)
    event.put("du", 5)
    event.put("category", category)
    val kv = new Gson().toJson(event)

    s"$user|$channel|$time|$kv"

  }


}
