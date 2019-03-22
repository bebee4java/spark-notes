package org.spark.notes

import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  *
  * @author sgr
  * @version 1.0, 2019-03-22 13:39
  **/
object DataSourceJDBCTable {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataSourceJDBCTable")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    val mysqlConf = scala.collection.mutable.Map(
      "url" -> "jdbc:mysql://localhost:3306/spark_war",
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "spark_war",
      "password" -> "123456",
      "dbtable" -> "test"
    )

    val test = spark.read.format("jdbc").options(mysqlConf).load()
    test.printSchema()
    //root
    // |-- id: long (nullable = false)
    // |-- name: string (nullable = true)
    // |-- age: integer (nullable = true)
    // |-- sex: integer (nullable = true)
    test.show()
    //+---+----+---+---+
    //| id|name|age|sex|
    //+---+----+---+---+
    //|  1|  zs| 20|  0|
    //|  2|  ls| 23|  1|
    //|  3|  ww| 21|  1|
    //|  4|  zs| 22|  1|
    //|  5|  zl| 19|  0|
    //+---+----+---+---+

    mysqlConf("dbtable") =
      """
        |(select * from test where sex=1) test
      """.stripMargin // 必须指定别名

    // 用户对关系型表数据筛选拉取 这很有用对只想取部分数据时
    val test1 = spark.read.format("jdbc").options(mysqlConf).load()

    test1.show()

    test1.createOrReplaceTempView("man")

    // 将mysql表加载成临时表与hive的表进行关联操作
    val result = spark.sql(
      """
        |select a.*,b.address from man a left join test.person_pt b
        |on b.sex = 1 and a.id = b.id
        |where a.age > 10
      """.stripMargin)

    result.show()

    result.select("name","age", "sex") // 字段名、类型和目标表要对于 可少不能多
      .write
      .format("jdbc")
      .mode(SaveMode.Append)
      .options(mysqlConf).
      option("dbtable", "test") //重新追加到mysql test表中
      .save()

    spark.close()
  }


}
