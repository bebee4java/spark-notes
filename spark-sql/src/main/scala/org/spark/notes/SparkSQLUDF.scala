package org.spark.notes

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StringType}

/**
  *
  * @author sgr
  * @version 1.0, 2019-03-22 15:06
  **/
object SparkSQLUDF {

  val data = Seq(
    "{\"id\":1, \"date\":\"2019-02-20 12:30:00\",\"studentId\":111,\"language\":68,\"math\":69,\"english\":90.2,\"classId\":\"Class1\",\"departmentId\":\"Economy\"}",
    "{\"id\":2, \"date\":\"2019-01-21 12:31:00\",\"studentId\":112,\"language\":73,\"math\":80,\"english\":96.1,\"classId\":\"Class1\",\"departmentId\":\"Economy\"}",
    "{\"id\":3, \"date\":\"2019-03-20 12:30:00\",\"studentId\":113,\"language\":90,\"math\":74,\"english\":75,\"classId\":\"Class1\",\"departmentId\":\"Economy\"}",
    "{\"id\":4, \"date\":\"2019-02-20 12:30:00\",\"studentId\":114,\"language\":89,\"math\":94,\"english\":93.5,\"classId\":\"Class1\",\"departmentId\":\"Economy\"}",
    "{\"id\":5, \"date\":\"2019-03-23 13:33:00\",\"studentId\":115,\"language\":99,\"math\":93,\"english\":89,\"classId\":\"Class1\",\"departmentId\":\"Economy\"}",
    "{\"id\":6, \"date\":\"2019-03-20 12:30:00\",\"studentId\":121,\"language\":96,\"math\":74,\"english\":79,\"classId\":\"Class2\",\"departmentId\":\"Economy\"}",
    "{\"id\":7, \"date\":\"2019-03-20 12:30:00\",\"studentId\":122,\"language\":89,\"math\":86,\"english\":85.0,\"classId\":\"Class2\",\"departmentId\":\"Economy\"}",
    "{\"id\":8, \"date\":\"2019-03-24 14:34:00\",\"studentId\":123,\"language\":70,\"math\":78,\"english\":61,\"classId\":\"Class2\",\"departmentId\":\"Economy\"}",
    "{\"id\":9, \"date\":\"2019-03-20 12:30:00\",\"studentId\":124,\"language\":76,\"math\":70,\"english\":76,\"classId\":\"Class2\",\"departmentId\":\"Economy\"}",
    "{\"id\":10,\"date\":\"2019-03-20 12:30:00\",\"studentId\":211,\"language\":89,\"math\":93,\"english\":60.4,\"classId\":\"Class1\",\"departmentId\":\"English\"}",
    "{\"id\":11,\"date\":\"2019-03-25 15:30:00\",\"studentId\":212,\"language\":76,\"math\":83,\"english\":75,\"classId\":\"Class1\",\"departmentId\":\"English\"}",
    "{\"id\":12,\"date\":\"2019-02-20 12:35:00\",\"studentId\":213,\"language\":71,\"math\":94,\"english\":90,\"classId\":\"Class1\",\"departmentId\":\"English\"}",
    "{\"id\":13,\"date\":\"2019-03-20 12:30:00\",\"studentId\":214,\"language\":94,\"math\":94,\"english\":66.0,\"classId\":\"Class1\",\"departmentId\":\"English\"}",
    "{\"id\":14,\"date\":\"2019-03-27 17:30:00\",\"studentId\":215,\"language\":84,\"math\":82,\"english\":73,\"classId\":\"Class1\",\"departmentId\":\"English\"}",
    "{\"id\":15,\"date\":\"2019-03-20 12:30:00\",\"studentId\":216,\"language\":85,\"math\":74,\"english\":93.3,\"classId\":\"Class1\",\"departmentId\":\"English\"}",
    "{\"id\":16,\"date\":\"2019-02-28 12:30:00\",\"studentId\":221,\"language\":77,\"math\":99,\"english\":61.4,\"classId\":\"Class2\",\"departmentId\":\"English\"}",
    "{\"id\":17,\"date\":\"2019-03-20 12:38:00\",\"studentId\":222,\"language\":80,\"math\":78,\"english\":96,\"classId\":\"Class2\",\"departmentId\":\"English\"}",
    "{\"id\":18,\"date\":\"2019-03-29 12:30:00\",\"studentId\":223,\"language\":79,\"math\":74,\"english\":96,\"classId\":\"Class2\",\"departmentId\":\"English\"}",
    "{\"id\":19,\"date\":\"2019-02-20 18:31:00\",\"studentId\":224,\"language\":75,\"math\":80,\"english\":78.0,\"classId\":\"Class2\",\"departmentId\":\"English\"}",
    "{\"id\":20,\"date\":\"2019-03-27 19:30:00\",\"studentId\":225,\"language\":82,\"math\":85,\"english\":63.8,\"classId\":\"Class2\",\"departmentId\":\"English\"}"
  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSQLUDF").master("local[2]").getOrCreate()

    import spark.implicits._
    val ds = spark.createDataset(data)

    val df = spark.read.json(ds)

    df.printSchema()
    df.show()

    df.createOrReplaceTempView("student_info")

    import org.apache.spark.sql.functions._
    // 内置udf函数：将english成绩四舍五入 转成int
    df.select(df("date"), df("studentId"),
      round(df("english")).cast("int").as("english")).show()


    // 自定义udf 时间转换函数date_format
    // 注册成函数
    spark.udf.register("my_date_format",format_date)
    spark.sql(
      """
        |select my_date_format(date,"yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd") as date,
        | studentId,int(round(english)) as english
        | from student_info
      """.stripMargin)
      //.show()

    // 或者创建一个udf对象
    df.select(df("studentId"), df("date"),
      round(df("english")).cast("int").as("english"))
      .withColumn("date", my_date_format(df("date"),lit("yyyy-MM-dd HH:mm:ss"), lit("yyyy-MM-dd")))
      //.show()

    df.select(df("studentId"),
      tag_socre(df("math")).as("math"),
      my_date_format(df("date"),lit("yyyy-MM-dd HH:mm:ss"), lit("yyyy-MM-dd")).as("date"))
      .show()

    // 注册成udf函数 比较简单
    spark.udf.register("tag_socre", udf[String,Long](score_tag))

    df.select(udf{score_tag _ }.apply(df("math")).as("math")).show()

  }

  val dateFormat = new SimpleDateFormat()

  val format_date:(String, String, String) => String =
    (date:String, formFormat:String, toFormat:String) => {
      dateFormat.applyPattern(formFormat)
      val d:Date = dateFormat.parse(date)
      dateFormat.applyPattern(toFormat)
      dateFormat.format(d)
    }

  import org.apache.spark.sql.functions._
  val my_date_format = udf(format_date)


  def score_tag(socre:Long):String = {
    socre match {
      case socre if(socre<60) => "不及格"
      case socre if(socre>=60 && socre<80) => "一般"
      case socre if(socre>=80) => "优秀"
    }
  }
  import org.apache.spark.sql.functions._
  val tag_socre = udf(score_tag _, StringType)


}
