package org.spark.war.common

import java.io.File


/**
  *
  * @author sgr
  * @version 1.0, 2019-03-03 01:35
  **/
object CommonUtil {

  def getBinDir():String = System.getProperty("user.dir")

  def getWorkingDir():String = new File(getBinDir()).getAbsolutePath

  def getConfDir():String = new File(getWorkingDir(), "config").getAbsolutePath

  def getDataDir():String = new File(getWorkingDir(), "data").getAbsolutePath



}
