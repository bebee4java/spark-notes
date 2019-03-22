package org.spark.notes

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  *
  * @author sgr
  * @version 1.0, 2019-03-17 15:26
  **/
object RDD2DataSet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("RDD2DataSet").master("local[2]").getOrCreate()
    val rdd = spark.sparkContext.textFile("./spark-sql/data/people.txt")

    import spark.implicits._
    val ds = rdd.map(_.split(",")).map(line => Person(line(0).toInt, line(1), line(2).toInt)).toDS

    ds.printSchema()
    ds.filter("age > 19").show()

    // DS 转 RDD
    val rddPerson:RDD[Person] = ds.rdd

    // DS 转 DF
    val df = ds.toDF
    df.printSchema()

    spark.close()
  }
  case class Person(id:Int, name:String, age:Int)

}
