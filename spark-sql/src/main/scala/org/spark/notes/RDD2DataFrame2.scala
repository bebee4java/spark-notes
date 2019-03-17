package org.spark.notes

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}


/**
  *
  * @author sgr
  * @version 1.0, 2019-03-17 14:26
  **/
object RDD2DataFrame2 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("RDD2DataFrame2").master("local[2]").getOrCreate()
    val rdd = spark.sparkContext.textFile("./spark-sql/data/people.txt")

    val rddRow:RDD[Row] = rdd.map(_.split(",")).map(line => Row(line(0).toInt, line(1), line(2).toInt))

    val structType = StructType(StructField("id", IntegerType, false)
      :: StructField("name", StringType, false)
      :: StructField("age", IntegerType, false)
      :: Nil)


    val df = spark.createDataFrame(rddRow, structType)
    df.printSchema()

    df.createOrReplaceTempView("people")

    spark.sql("select age,count(1) cnt from people group by age").show()


  }
}
