package org.spark.notes

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark RDD 测试
  *
  * @author sgr
  * @version 1.0, 2019-02-22 02:20
  **/
object SparkRDDTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkRDDTest").setMaster("local[2]")
    val sparkContext = new SparkContext(sparkConf)

    /**
      *  通过sparkContext的parallelize将集合转成RDD
      *  parallelize 可以指定并行度numSlices 默认为2
      */
    val rdd:RDD[Int] = sparkContext.parallelize(Array(1,2,3)) // 返回的是RDD[Int]类型

    val cnt = rdd.count()
    println(s"rdd 数据条数为:$cnt")

    sparkContext.stop() //程序结束 最好将sc关闭

  }

}
