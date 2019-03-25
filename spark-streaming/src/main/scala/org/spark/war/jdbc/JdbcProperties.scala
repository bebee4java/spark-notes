package org.spark.war.jdbc

/**
  * 数据库配置参数常量类
  * @author sgr
  * @version 1.0, 2019-03-03 17:06
  **/
object JdbcProperties {
  private[jdbc] val DATASOURCE = "datasource"
  private[jdbc] val DATASOURCE_DRIVER = "driver"
  private[jdbc] val DATASOURCE_URL = "url"
  private[jdbc] val DATASOURCE_USERNAME = "username"
  private[jdbc] val DATASOURCE_PASSWORD = "password"
  private[jdbc] val DATASOURCE_MAXACTIVE = "maxactive"
  private[jdbc] val DATASOURCE_MAXIDLE = "maxidle"
  private[jdbc] val DATASOURCE_MAXWAIT = "maxwait"
  private[jdbc] val DATASOURCE_VALIDATIONQUERY = "validationQuery"
}
