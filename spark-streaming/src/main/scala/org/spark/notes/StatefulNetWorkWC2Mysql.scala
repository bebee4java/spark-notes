package org.spark.notes

import java.sql.{Connection, DriverManager}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.{ConnectionPool, DB, SQL, using}

/**
  * 有状态的累计接收socket流数据单词统计结果入mysql
  *
  * @author sgr
  * @version 1.0, 2019-02-18 14:12
  **/
object StatefulNetWorkWC2Mysql {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("StatefulNetWorkWC2Mysql").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val lines = ssc.socketTextStream("localhost", 5678)
    val dstream = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)

    /*
    该方式有问题 会报：
    org.apache.spark.SparkException: Task not serializable

    dstream.foreachRDD(rdd => {
      val connection = getConnection()  // executed at the driver
        rdd.foreach { record => {
          val word = record._1
          val count = record._2.toInt
          connection.createStatement().execute(s"insert into wordcount(word, count) values('$word', $count)")
        // executed at the worker
        }
      }
    })*/

    /*
    该方式有问题：
    每条记录都会创建一个连接

    dstream.foreachRDD(rdd => {
        rdd.foreach { record => {
          val connection = getConnection()
          val word = record._1
          val count = record._2.toInt
          connection.createStatement().execute(s"insert into wordcount(word, count) values('$word', $count)")
        // executed at the worker
        }
      }
    })*/

    /*dstream.foreachRDD { rdd =>
      rdd.foreachPartition { partitionOfRecords => {
          val connection = getConnection()
          partitionOfRecords.foreach(record => {
            val word = record._1
            val count = record._2.toInt
            val sql =
              s"""
                 |INSERT INTO `wordcount`(`word`, `count`) VALUES('$word', $count)
                 |  ON DUPLICATE KEY
                 |  UPDATE `count`=count+$count;
               """.stripMargin
            connection.createStatement().execute(sql)
            print("=======" + sql)
          })
          connection.close()
        }
      }
    }*/

    // 更效率的方式：连接池
    dstream.foreachRDD {rdd =>
      rdd.foreachPartition {
        partitionOfRecords => {
          DbUtils.init("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/test",
            "root", "253824")
          partitionOfRecords.foreach(record => {
            val word = record._1
            val count = record._2.toInt
            val sql =
              s"""
                 |INSERT INTO `wordcount`(`word`, `count`) VALUES('$word', $count)
                 |  ON DUPLICATE KEY
                 |  UPDATE `count`=count+$count;
               """.stripMargin
            DbUtils.exec_insert_sql(sql)
            println("============")
          })

        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }


  def getConnection(): Connection ={
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/test",
      "root", "253824")
  }

}

object DbUtils {
  private var driverClass: String = _
  private var url: String = _
  private var username: String = _
  private var password: String = _

  private var connectionPool: ConnectionPool = _


  def init(driverClass: String, url: String, username: String, password: String) = {
    this.driverClass = driverClass
    this.url = url
    this.username = username
    this.password = password

    Class.forName(driverClass)
    ConnectionPool.singleton(url, username, password)
    connectionPool = ConnectionPool.apply()
  }

  def exec_insert_sql(_sql: String) = {
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

  def exec_select_sql(_sql: String, columns: List[String]) = {
    using(DB(this.connectionPool.borrow())) {
      db => {
        db.localTx {
          implicit session => {
            SQL(_sql).map(records => {
              val record = new Array[String](columns.size)
              var i = 0
              for (column <- columns) {
                record(i) = records.get(column)
                i += 1
              }
              record
            }).list().apply()
          }
        }
      }
    }
  }
}
