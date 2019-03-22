package org.spark.notes

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  *
  * @author sgr
  * @version 1.0, 2019-03-21 16:59
  **/
object DataSourceParquet {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataSourceParquet").master("local[2]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val df = spark.read.format("csv").option("header", "true").load("./spark-sql/data/users.csv")

    df.printSchema()
    //root
    // |-- id: string (nullable = true)
    // |-- name: string (nullable = true)
    // |-- age: string (nullable = true)
    // |-- sex: string (nullable = true)

    df.write
      .partitionBy("sex")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("./spark-sql/data/output/parquet")
    //data
    //    └── output
    //        └── parquet
    //            ├── sex=0
    //            │   ├── ...
    //            └── sex=1
    //                ├── ...

    df.write
      .bucketBy(3,"age") //将会产生3份文件
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path","./spark-sql/spark-warehouse/users_parquet")  // 自定义表路径
      .saveAsTable("users_parquet")

    spark.sql("desc formatted users_parquet").show(false)
    spark.sql("select age,count(1) cnt from users_parquet group by age").show()

    spark.close()
  }

}
