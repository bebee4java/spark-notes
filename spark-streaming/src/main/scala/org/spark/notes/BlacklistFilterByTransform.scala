package org.spark.notes

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 通过transform操作进行黑名单的过滤操作
  *
  * @author sgr
  * @version 1.0, 2019-02-19 00:28
  **/
object BlacklistFilterByTransform {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("BlacklistFilterByTransform").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val blacklist = List("zs", "ls")
    // 黑名单：（zs,true）(ls,true)
    val blackRdd = ssc.sparkContext.parallelize(blacklist).map((_, true))

    // 流记录：1,zs 2,ls 3,ww
    val lines = ssc.socketTextStream("localhost",6789)
    val normallist = lines.map(x => (x.split(",")(1), x))  // (zs,'1,zs') (ls, '2,ls')
      .transform(
      rdd => {
        rdd.leftOuterJoin(blackRdd) // (zs,('1,zs',true)) (ww,('3,ww', None))
          .filter(x => x._2._2.getOrElse(false) == false)
          .map(x => x._2._1)  //'1,zs' '2,ls'
      }
    )

    normallist.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
