package org.spark.notes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  *
  * @author sgr
  * @version 1.0, 2019-03-22 19:38
  **/
object SparkSQLUDAF {
  val data = SparkSQLUDF.data

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSQLUDAF").master("local[2]").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._
    val ds = spark.createDataset(data)
    val df = spark.read.json(ds)
    df.createOrReplaceTempView("student_info")

    println(df.count())

    spark.udf.register("mycount", myCount)

    // 可以自定义 null时也+1
    spark.sql("select mycount(1) cnt1,mycount(id) cnt2,mycount(null) cnt3 from student_info")
      //.show()

    spark.sql("select classId,mycount(1) cnt from student_info group by classId").show()

    spark.udf.register("myavg", myAvg)
    spark.sql("select classId, avg(math) avg_math from student_info group by classId").show()

    spark.sql("select classId, myavg(math) avg_math from student_info group by classId").show()


  }
}

object myCount extends UserDefinedAggregateFunction {
  //聚合函数的输入参数数据类型
  override def inputSchema: StructType = StructType(
    StructField("inputColumn", StringType) ::
    StructField("inputColumn", LongType) :: Nil
  )

  //中间缓存的数据类型
  override def bufferSchema: StructType = StructType(
    StructField("count", LongType) :: Nil
  )

  //最终输出结果的数据类型
  override def dataType: DataType = LongType

  //此函数是否总是在相同的输入上返回相同的输出
  override def deterministic: Boolean = true

  //初始值，要是DataSet没有数据，就返回该值
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
  }

  /**
    * 更新给定缓冲区
    * @param buffer 相当于当前分区的值，每行数据都需要进行计算，计算的结果保存到buffer中
    * @param input 新的输入数据
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // 计算总的个数
    if (input.anyNull) return

    buffer(0) = buffer.getLong(0) + 1
  }

  /**
    * 相当于把每个分区的数据进行汇总
    * @param buffer1  分区一的数据
    * @param buffer2  分区二的数据
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
  }

  //计算最终的结果
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0)
  }
}

object myAvg extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(
    StructField("inputColumn", LongType) :: Nil
  )

  override def bufferSchema: StructType = StructType(
    StructField("sum", LongType) ::
    StructField("count", LongType) :: Nil
  )

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L //sum
    buffer(1) = 0L //count
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (input.anyNull) return
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // sum 合并
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    //count 合并
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Any = {
    // 计算最终的avg
    buffer.getLong(0).toDouble / buffer.getLong(1).toDouble
  }
}


