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

    println(rdd.getClass.getSimpleName)  //ParallelCollectionRDD

    val cnt = rdd.count()
    println(s"rdd 数据条数为:$cnt")

    // 目录下有子目录就不能只写到目录，但可以通过正则取所有文件
    val txt = sparkContext.textFile("spark基础概念/data/*txt")
    txt.foreach(line => println(line))

    println(txt.getClass.getSimpleName) //MapPartitionsRDD

    // 会主动去找指定目录下的文件，忽略子目录
    val alltxt = sparkContext.wholeTextFiles("spark基础概念/data")

    alltxt.foreach(line => println(line))

    sparkContext.stop() //程序结束 最好将sc关闭

  }

}
