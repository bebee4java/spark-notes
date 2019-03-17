package org.spark.notes

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}


/**
  *
  * @author sgr
  * @version 1.0, 2019-03-17 14:26
  **/
object RDD2DataFrame1 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("RDD2DataFrame1").master("local[2]").getOrCreate()
    val rdd = spark.sparkContext.textFile("./spark-sql/data/people.txt")

    import spark.implicits._
    val df = rdd.map(_.split(",")).map(line => Person(line(0).toInt, line(1), line(2).toInt)).toDF
    df.printSchema()

    df.createOrReplaceTempView("people")

    val teenagersDF = spark.sql("select * from people  where age between 18 and 30")
    teenagersDF.show()

    // DF 转 RDD
    val rddRow:RDD[Row] = df.rdd
    // DF 转 DS
    val ds:Dataset[Person] = df.as[Person]
  }
  case class Person(id:Int, name:String, age:Int)
}
