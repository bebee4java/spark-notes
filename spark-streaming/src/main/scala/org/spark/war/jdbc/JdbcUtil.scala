package org.spark.war.jdbc

import org.spark.war.common.{ParamMapUtil, WarContext}
import org.spark.war.log.Logger
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings, DB, SQL, using}
import java.util


/**
  * 数据库工具类
  * @author sgr
  * @version 1.0, 2019-03-03 17:05
  **/
object JdbcUtil {
  private val logger:Logger = Logger.getInstance(JdbcUtil.getClass)
  private val jdbcConf = WarContext.getMap(JdbcProperties.DATASOURCE)
  private val driver = ParamMapUtil.getStringValue(jdbcConf, JdbcProperties.DATASOURCE_DRIVER)
  private val url = ParamMapUtil.getStringValue(jdbcConf, JdbcProperties.DATASOURCE_URL)
  private val username = ParamMapUtil.getStringValue(jdbcConf, JdbcProperties.DATASOURCE_USERNAME)
  private val password = ParamMapUtil.getStringValue(jdbcConf, JdbcProperties.DATASOURCE_PASSWORD)
  private val maxactive = ParamMapUtil.getIntValue(jdbcConf, JdbcProperties.DATASOURCE_MAXACTIVE, 30)
  private val maxwait = ParamMapUtil.getLongValue(jdbcConf, JdbcProperties.DATASOURCE_MAXWAIT, -1L)
  private val validationQuery = ParamMapUtil.getStringValue(jdbcConf, JdbcProperties.DATASOURCE_VALIDATIONQUERY,
      "select 1 from dual"
  )

  def init():ConnectionPool = {
    try {
      logger.info("JdbcUtil init datasource start...")
      Class.forName(driver)
      val setting = ConnectionPoolSettings(initialSize = 10, maxSize = maxactive,
        connectionTimeoutMillis = maxwait, validationQuery = validationQuery
      )
      ConnectionPool.singleton(url, username, password, setting)
      logger.info("JdbcUtil init datasource end...")
    } catch {
      case ex:Exception => throw new Exception("datasource init failed!!", ex)
    }
    ConnectionPool.apply()

  }

  private val connectionPool = init()


  def update(_sql:String) = {
    using(DB(this.connectionPool.borrow())) {
      db => {
        db.localTx {
          implicit session => {
            SQL(_sql).update().apply()
          }
        }
      }
    }
  }

  def update(_sql:String, seq: Seq[Seq[_]]) = {
    using(DB(this.connectionPool.borrow())) {
      db => {
        db.localTx {
          implicit session => {
            /**
              * :_* 是类型ascription的特殊实例，它告诉编译器将序列类型的单个参数视为可变参数序列，例如varargs
              * 这是一种特殊的表示法，它告诉编译器将每个元素作为自己的参数传递，而不是将所有元素作为单个参数传递。
              * 例子：sum(args: Int*) sum(1), sum(1,2)这是普遍的传递参数方式
              * 我们也可以这么做定义 xs = List(1,2,3) sum(xs: _*)
              */
            SQL(_sql).batch(seq:_*).apply()
          }
        }
      }
    }
  }

  def query(_sql:String):List[util.HashMap[String,Any]] = {
    using(DB(this.connectionPool.borrow())) {
      db => {
        db.localTx {
          implicit session => {
            SQL(_sql).map(records => {
              val meta = records.metaData
              val cnt = meta.getColumnCount
              val map = new util.HashMap[String,Any](cnt)
              for (i <- 1 to cnt){
                val colType = meta.getColumnTypeName(i)
                val colName = meta.getColumnName(i)
                val obj =
                colType match {
                  case "BIGINT" => records.long(colName)
                  case "INT" => records.int(colName)
                  case "VARCHAR" => records.string(colName)
                  case i if i.startsWith("TINYINT") => records.underlying.getInt(colName)
                  case _ => records.string(colName)
                }
                map.put(colName, obj)
              }
              map
            }).list().apply()
          }
        }
      }
    }
  }

}
