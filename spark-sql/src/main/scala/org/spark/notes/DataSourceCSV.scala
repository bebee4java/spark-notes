package org.spark.notes

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  *
  * @author sgr
  * @version 1.0, 2019-03-21 23:22
  **/
object DataSourceCSV {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataSourceCSV").master("local[2]").getOrCreate()
    val df = spark.read
        .option("delimiter", "\t") // 指定分割符
        .option("header", true)  // 带字段头
        .option("multiLine", true)  //字段值有换行
        .csv("./spark-sql/data/test.csv")
    df.printSchema()
    //root
    // |-- id: string (nullable = true)
    // |-- name: string (nullable = true)
    // |-- age: string (nullable = true)
    // |-- sex: string (nullable = true)
    // |-- remark: string (nullable = true)

    df.show()
    //+---+----+---+---+--------+
    //| id|name|age|sex|  remark|
    //+---+----+---+---+--------+
    //|  1|  zs| 20|  0|    null|
    //|  2|  ls| 22|  1|      哈哈|
    //|  3|  ww| 20|  1|很优秀
    //年轻有为|
    //|  4|  zl| 23|  0|    null|
    //|  5|  zs| 28|  0|      88|
    //+---+----+---+---+--------+

    import org.apache.spark.sql.functions._
    import spark.implicits._
    val df1 = df.select($"id", $"name", $"age", $"sex",
      translate($"remark","\n",",").as("remark"))
      .where(not(isnull($"remark")))
    // 筛选出有备注的记录，并且将换行替换成,

    df1.show()
    //+---+----+---+---+--------+
    //| id|name|age|sex|  remark|
    //+---+----+---+---+--------+
    //|  2|  ls| 22|  1|      哈哈|
    //|  3|  ww| 20|  1|很优秀,年轻有为|
    //|  5|  zs| 28|  0|      88|
    //+---+----+---+---+--------+

    df1.write
      .option("header", true)
      .mode(SaveMode.Overwrite)
      .csv("./spark-sql/data/output/csv/")

  }

}
