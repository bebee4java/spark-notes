package org.spark.notes

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author sgr
  * @version 1.0, 2019-03-16 21:27
  **/
object ShuffleOptimize {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("ShuffleOptimize").setMaster("local[2]")
    val sparkContext = new SparkContext(sparkConf)

    // 订单信息 超大 (id, date, pid)
    val orderList = List(
      (1, "2019-03-13","001"),
      (2, "2019-03-13","001"),
      (3, "2019-03-13","002"),
      (4, "2019-03-13","001"),
      (5, "2019-03-13","002"),
      (6, "2019-03-13","001"),
      (7, "2019-03-13","002"),
      (8, "2019-03-13","001"),
      (9, "2019-03-13","001"),
      (10, "2019-03-13","001")
    )

    // 商品信息 极少 (pid, pname)
    val productList = List(("001", "小米手机"), ("002", "华为手机"))

    val rddA = sparkContext.parallelize(orderList, 3)
    val rddB = sparkContext.parallelize(productList)

    // 执行的时候超大的A被打散和分发到各个节点去 产生shuffle
    rddA.map(x => (x._3, x)).leftOuterJoin(
      rddB
    ).map(x => (x._2._1._1, x._2._1._2, x._2._2.get)).foreach(println)

    // 优化后的代码：
    // 全量的B被分发到A的数据节点，A直接处理map，一次shuffle都没有
    val product = sparkContext.broadcast(productList)

    rddA.map{
      case (id, date, pid) =>
        val pnames = product.value.toMap
        if (product.value.map(_._1).contains(pid)){
          (id, date, pnames.get(pid))
        } else {
          (id, date, null)
        }
    }.map(x => (x._1, x._2, x._3.get)).foreach(println)

  }

}
