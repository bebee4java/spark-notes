package org.spark.notes

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  *
  * @author sgr
  * @version 1.0, 2019-03-22 00:15
  **/
object DataSourceHiveTable {
  def main(args: Array[String]): Unit = {
    // 需要将core-site.xml hdfs-site.xml hive-site.xml配置文件放到resources目录下
    val spark = SparkSession.builder()
      .appName("DataSourceHiveTable")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()
    spark.sql("show databases").show()
    //+------------+
    //|databaseName|
    //+------------+
    //|     default|
    //|        test|
    //+------------+

    spark.sql("select id,name,age from test.person_pt").show()
    //+---+----+---+
    //| id|name|age|
    //+---+----+---+
    //|  5|  zy| 23|
    //|  1|  zs| 20|
    //|  3|  ww| 20|
    //|  2|  ls| 19|
    //|  4|  zf| 20|
    //|  6|  lb| 21|
    //+---+----+---+

    val df = spark.read
      .option("delimiter", "\t") // 指定分割符
      .option("header", true)  // 带字段头
      .option("multiLine", true)  //字段值有换行
      .csv("file:///Users/sgr/develop/ideaProject/spark-notes/spark-sql/data/test.csv")
    df.printSchema()
    // 会根据上面的df的schema自动创建hive的表
    // 默认为MANAGED TABLE 采用Parquet存储格式
    df.write.mode(SaveMode.Overwrite).saveAsTable("test.spark_sql_test")

    // 自定义存储path 将会创建hive外部表
    df.write.mode(SaveMode.Overwrite)
      .option("path", "hdfs://localhost:9000/usr/hive/warehouse/test.db/spark_sql_ext_test")
        .saveAsTable("test.spark_sql_ext_test")

    // 下面会自动以age做分区字段 创建分区表
    df.write.partitionBy("age").mode(SaveMode.Overwrite).saveAsTable("test.spark_sql_pt_test")

    spark.sql("select id,name from test.spark_sql_test").show()

    spark.close()
  }

}
