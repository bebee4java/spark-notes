package org.spark.notes

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types._

/**
  *
  * @author sgr
  * @version 1.0, 2019-03-21 20:34
  **/
object DataSourceJson {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataSourceJson").master("local[2]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val jsons = Array(
      "{\"a\":\"a1\",\"b\":[1,1]}",
      "{\"a\":\"a2\",\"b\":[2,2]}",
      "{\"a\":\"a3\",\"b\":[3]}",
      "{\"a\":\"a4\",\"b\":[4]}"
    )

    import spark.implicits._
    val ds = spark.sparkContext.parallelize(jsons).toDS
    val df = spark.read.json(ds)
    df.printSchema()
    //root
    // |-- a: string (nullable = true)
    // |-- b: array (nullable = true)
    // |    |-- element: long (containsNull = true)
    df.select("a","b").show()


    // 处理多行的json文件
    val person = spark.read
        .option("multiLine", true)
        .json("./spark-sql/data/person.json")
    person.printSchema()
    //root
    // |-- age: long (nullable = true)
    // |-- id: long (nullable = true)
    // |-- name: string (nullable = true)

    person.show()

    // 处理嵌套的json

    val json = Seq(
      """
        |{
        |"id":1,
        |"students":{
        |   "001":{
        |     "name":"zs",
        |     "score":435
        |   },
        |   "002":{
        |     "name":"ls",
        |     "score":535
        |   },
        |   "003":{
        |     "name":"ww",
        |     "score":635
        |   }
        |}
        |}
      """.stripMargin
    )

    val studentDS:Dataset[String] = spark.createDataset(json)

    val schema = new StructType()
      .add("id", LongType)
      .add("students",
        MapType(StringType,
          new StructType()
            .add("name", StringType)
            .add("score", LongType)
        )
      )

    val studentDF = spark.read.schema(schema).json(studentDS)

    studentDF.printSchema()
    //root
    // |-- id: long (nullable = true)
    // |-- students: map (nullable = true)
    // |    |-- key: string
    // |    |-- value: struct (valueContainsNull = true)
    // |    |    |-- name: string (nullable = true)
    // |    |    |-- score: long (nullable = true)

    studentDF.show(false)
    //+---+------------------------------------------------------+
    //|id |students                                              |
    //+---+------------------------------------------------------+
    //|1  |Map(001 -> [zs,435], 002 -> [ls,535], 003 -> [ww,635])|
    //+---+------------------------------------------------------+

    spark.close()
  }

}
