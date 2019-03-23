package org.spark.notes

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.{After, Before, Test}

/**
  *
  * @author sgr
  * @version 1.0, 2019-03-19 11:37
  **/

@Test
class DataFrameOperate {
  var spark:SparkSession = null
  var df:DataFrame = _

  @Before
  def init() = {
    spark = SparkSession.builder().appName("DataFrameOperate").master("local[2]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")


    df = spark.read.csv("./data/people.csv").toDF("id", "name", "age")


    df.printSchema()
  }

  @Test
  def metaInfo() = {
    // 打印schema
    df.printSchema()

    // 查看列的类型
    println(df.dtypes.mkString(","))

    // 查看列名
    println(df.columns.mkString(","))

    // 查看行数
    println(df.count)

  }

  @Test
  def columnRename() = {
    // 使用selectExpr方法 ==> 这种方式会返回新的DF只有uid一列
    val df1 = df.selectExpr("id as uid")
    df1.show()

    // withColumnRenamed方法 ==> 这种方式会返回新的DF包含所有列  推荐
    val df2 = df.withColumnRenamed("id" , "uid")
    df2.show()

    // alias 方法 ==> 这种方式会返回新的DF只有uid一列
    val df3 = df.select(df("id").alias("uid"))
    df3.show()

    df.select(df("id") as "uid", df("id").as("uid2")).show()

  }

  @Test
  def selection() = {
    val sql = spark.sqlContext
    import sql.implicits._

    // 选择几列的方法
    df.select("id", "name", "age").show(3)
    df.select(df("id"), df("name"), df("age")).show(true)
    df.select($"id", $"name", $"age").show(3,true)

    // 多列选择和切片
    df.select("id", "name", "age").select(df("age") > 10).show()
    // between 范围选择
    df.filter(df("age").between(10, 20)).select("id", "name", "age").show()
    // 联合筛选
    df.filter(df("age") > 10).filter(df("name").isin("wangwu","lisi")).show()

    // filter运行类SQL
    df.filter("age > 10 and name in('wangwu', 'lisi')").show()

    // where方法的SQL
    df.where("age > 10 and name in('wangwu', 'lisi')").show()
  }

  @Test
  def modifyColumn() = {
    val sql = spark.sqlContext
    import sql.implicits._
    // 删除列
    df.drop("id").show()

    // 增加列
    df.withColumn("newAge", df("age") + 10).show()

    import org.apache.spark.sql.functions._
    df.withColumn("newAge", col("age") + 10).show()

    // 增加常量列
    df.selectExpr("*", "1 one").show()

    // 通过lit函数增加列
    df.select(df("id"), lit(1).as("one")).show()

  }

  @Test
  def toJson() = {
    val p = (x:String) => println(x)
    df.toJSON.foreach(p)
  }


  @Test
  def sort() = {
    val sql = spark.sqlContext
    import sql.implicits._

    // 默认升序
    df.sort("age").show()

    // 降序排序
    df.sort($"age".desc).show()

    //多字段排序 混合排序
    df.sort($"id".desc, $"age".asc).show()
  }

  @Test
  def dealNull() = {

    // null 统一替换成 ""
    df.na.fill("").show()

    // 根据不同列进行null值替换
    df.na.fill(Map("name"->"", "age"-> -1)).show()

    df.selectExpr("id", "coalesce(name, '') as name", "coalesce(age, -1) as age").show()

  }

  @Test
  def typeCast() = {

    val df1 = df.selectExpr("cast(id as int) as id", "name", "cast(age as int) age")

    val df2 = df.select(df("id").cast("int"), df("name"), df("age").cast("int"))

    df1.printSchema()
    df1.show()

    df2.printSchema()
    df2.show()
  }

  @Test
  def regexp_op() = {
    val sql = spark.sqlContext
    import sql.implicits._

    import org.apache.spark.sql.functions._
    df.select($"id",
      regexp_extract($"name", "([A-Za-z]+)(_[0-9]?)?", 1).as("name")
      , $"age").show()
    df.select($"id",
      regexp_replace($"name", "([A-Za-z]+)(_[0-9]+)?", "aaa").as("name")
      , $"age").show()
  }

  @Test
  def agg_op() = {
    import org.apache.spark.sql.functions._
    df.agg(max("age"), min("age")).show()

    df.agg(Map("age"->"max", "age"->"min")).show()

    df.agg(("age", "max"), ("age", "min")).show()

    df.groupBy("age").agg(min("id")).show()

    df.createOrReplaceTempView("people")

    spark.sql("select age,min(id) from people group by age").show()
  }


  @Test
  def window_func_op() = {
    val sql = spark.sqlContext
    import sql.implicits._
    import org.apache.spark.sql.functions._
    val df1 = df.select($"id", $"name", $"age",
      row_number().over(Window.partitionBy("age").orderBy($"id".asc)).as("rank")
    )
    df1.show()

    df1.filter("rank = 1").show()
  }

  @Test
  def join_op() = {
    val df1 = df.filter("id < 3")
    val df2 = df.na.drop()

    df1.show()
    df2.show()

//    df1.join(df2).show()

    // 默认inner
    df1.join(df2,"id").show()

    // `inner`, `cross`, `outer`, `full`, `full_outer`, `left`, `left_outer`,`right`, `right_outer`, `left_semi`, `left_anti`
    df1.join(df2, Seq("id"), "right").show()

    // 必须使用===代替等于
    df1.join(df2, df1("id") === df2("id") ).show()


    // 下面操作产生笛卡尔积 需要配置spark.sql.crossJoin.enabled=true 不然会报：
    // AnalysisException: Detected cartesian product for INNER join between logical plans
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    df1.join(df2, df1("id").>(df2("id"))).show() // 不好使

    df1.createOrReplaceTempView("df1")
    df2.createOrReplaceTempView("df2")
    // 建议直接使用sql
    spark.sql("select * from df1 a inner join df2 b on a.id > b.id").show()


    df1.crossJoin(df2).show() //笛卡尔积 n*n
    /**
      * 实现会把replace("_", "")
      * case "inner" => Inner
      * case "outer" | "full" | "fullouter" => FullOuter
      * case "leftouter" | "left" => LeftOuter
      * case "rightouter" | "right" => RightOuter
      * case "leftsemi" => LeftSemi
      * case "leftanti" => LeftAnti
      * case "cross" => Cross
      */
    df1.join(df2,Seq("id"),"full").show() //full 和 full_outer 一致

    df1.join(df2, Seq("id"), "leftsemi").show() // 取df1中在df2出现的记录
    df1.join(df2, Seq("id"), "leftanti").show() // 取df1中不在在df2出现的记录
  }

  @Test
  def union_op() = {
    val df1 = df.filter("id < 3")
    val df2 = df.na.drop()

    df1.show()
    df2.show()
    df1.union(df2).distinct().show()
//  结构必须一致
//    df.select("id","name").union(df.select("id")).show()
  }

  @After
  def shutDown() = {
    if (spark != null) spark.close()
  }

}
