package org.spark.notes

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, Test}


/**
  *
  * @author sgr
  * @version 1.0, 2019-03-14 16:59
  **/

@Test
class SparkRDDOperatorTest {
  private var sparkContext: SparkContext = null

  @Before
  def init() = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[2]")
    sparkConf.setAppName("SparkRDDOperatorTest")
    sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("WARN")
  }

  @After
  def shutdown() = {
    if (sparkContext != null) sparkContext.stop()
  }


  @Test
  def accumulator() = {
    val rdd = sparkContext.parallelize(Array(1, 2, 3))
    var counter = 0
    rdd.foreach(x => counter += x)
    //发送给每个执行器的闭包中的变量现在是副本，因此，当foreach函数中引用counter时，它不再是驱动程序节点上的计数器。
    // 驱动程序节点的内存中仍然有一个计数器，但执行者将无法再看到它！执行器只看到序列化闭包中的副本。
    // 因此，计数器的最终值仍然为零，因为计数器上的所有操作都引用了序列化闭包中的值。

    println(counter) // 结果还是0


    //为了确保在这些场景中定义良好的行为，应该使用累加器。Spark中的累加器专门用于提供一种机制，
    // 在集群中跨工作节点拆分执行时安全地更新变量。
    val longAccumulator = sparkContext.longAccumulator
    rdd.foreach(x => longAccumulator.add(x))
    println(longAccumulator.value)
  }


  @Test
  def map() = {
    val rdd = sparkContext.parallelize(Array(1, 2, 3))
    rdd.map(println) //不会执行 map是transformations算子

    // rdd每个元素加1
    rdd.map(_ + 1).foreach(println)
  }

  @Test
  def filter() = {
    val rdd = sparkContext.parallelize(Array(1, 2, 3, 4, 5))
    // 过滤出rdd所有偶数
    rdd.filter(_ % 2 == 0).foreach(println)
  }

  @Test
  def flatMap() = {
    val rdd = sparkContext.parallelize(Array(Array(1, 2), Array(3, 4)))

    // 每个元素+1 然后扁平化
    // flatmap 接收 f: T => Seq(U)
    rdd.flatMap(
      arr => {
        arr.map(_ + 1)
      }
    ).foreach(println)
  }

  @Test
  def mapPartitions() = {
    /**
      * 和map算子类似map算子一次处理一个partition的一条数据，而mapPartition算子一次处理一个partition的所有数据
      *
      * 使用场景：
      * 如果rdd的数据不是特别多的时候，采用mapPartition算子代替map算子可以提高处理速度
      * 但是rdd数据量特别大的时候不建议使用mapPartition算子，会造成内存溢出
      *
      * func必须是iterator[T]> => iterator[U]类型
      *
      */

    val rdd = sparkContext.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2)

    // 计算每个partition上的数据量
    val cnt = rdd.mapPartitions { its =>
      //注意Iterator 只能使用一次
      //      println(its.size)
      val list = its.toList
      println(list.size)
      list.iterator
    }.count()

    val count = rdd.count()

    println("====" + count, cnt)

  }

  @Test
  def mapPartitionsWithIndex() = {
    val rdd = sparkContext.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2)

    //打印rdd每个分区的索引号 和 对应的数据
    val cnt = rdd.mapPartitionsWithIndex {
      (index, its) =>
        val list = its.toList
        println("partition{} " + index + " data{} " + list)
        list.iterator
    }.count()
  }

  @Test
  def sample() = {
    val rdd = sparkContext.parallelize(Array(1, 2, 3, 4, 5))
    //采样取数 第一个参数：取出的元素是否放回 第二个参数：抽取比例 第三个参数：种子 如果写死每次抽样相同
    val rdd1 = rdd.sample(true, 0.8, 1)
    val rdd2 = rdd.sample(false, 0.8, 1)
    val rdd3 = rdd.sample(true, 0.8)

    rdd1.foreach(println)
    println("---------")
    rdd2.foreach(println)
    println("---------")
    rdd3.foreach(println)

    println("=========")

    rdd1.foreach(println)
    println("---------")
    rdd2.foreach(println)
    println("---------")
    rdd3.foreach(println)
  }

  @Test
  def union() = {
    val rdd1 = sparkContext.parallelize(Array(1, 2, 3, 4, 5), 2)
    val rdd2 = sparkContext.parallelize(Array(1,2, 3, 4, 5), 1)

    rdd1.mapPartitionsWithIndex{
      (index, its) =>
        println("rdd1===>" + index)
        its
    }.count()

    rdd2.mapPartitionsWithIndex{
      (index, its) =>
        println("rdd2===>" + index)
        its
    }.count()

    //union算子逻辑上的抽象，不会改变之前partition的个数

    val rdd3 = rdd1.union(rdd2)

    rdd3.foreach(println)

    rdd3.mapPartitionsWithIndex{
      (index, its) =>
        println("rdd3===>" + index)
        its
    }.count()

    println()

  }

  @Test
  def intersection() = {
    val rdd1 = sparkContext.parallelize(Array(1, 2, 3, 4, 5), 2)
    val rdd2 = sparkContext.parallelize(Array(3, 3, 6, 7), 1)

    rdd1.mapPartitionsWithIndex{
      (index, its) =>
        println("rdd1===>" + index)
        its
    }.count()

    rdd2.mapPartitionsWithIndex{
      (index, its) =>
        println("rdd2===>" + index)
        its
    }.count()

    //intersection算子可以重新修改partition个数

    val rdd3 = rdd1.intersection(rdd2)

    rdd3.foreach(println)

    rdd3.mapPartitionsWithIndex{
      (index, its) =>
        println("rdd3===>" + index)
        its
    }.count()

    println()

  }

  @Test
  def distinct() = {
    val rdd = sparkContext.parallelize(Array(1, 2, 3, 4, 5, 2, 3, 3, 5), 2)

    // 底层调用的是reduceByKey
    rdd.distinct().foreach(println)
  }


  @Test
  def groupBy() = {

    val rdd = sparkContext.parallelize(Array(1, 2, 3, 4, 5), 2)

    // 将rdd的数据按奇偶进行分组
    rdd.groupBy(x => if(x%2 ==0 ) 1 else 0).foreach{
      x =>
        println(x._1, x._2.toList)
    }

  }

  @Test
  def groupByKey() = {

    val rdd = sparkContext.parallelize(Array((1,"a"), (2,"b"), (3,"c"), (4,"d")), 2)

    // groupByKey 必须作用在pair格式（键值对）的RDD上
    // 默认采用HashPartitioner 当然也可以自定义Partitioner
    rdd.groupByKey().foreach{
      x =>
        println(x._1, x._2.toList)
    }

  }


  @Test
  def reduceByKey() = {

    val rdd = sparkContext.parallelize(Array((2,"a"), (2,"b"), (4,"c"), (4,"d")), 2)

    //reduceByKey算子 = groupByKey + reduce
    //spark里的reduceByKey在map端自带Combiner

    // reduceByKey 必须作用在pair格式（键值对）的RDD上
    // 按相同key进行数据的聚合
    // 此例将key一样的string值进行拼接
    rdd.reduceByKey(_ +"|"+ _).foreach{
      x =>
        println(x._1, x._2)
    }

  }

  @Test
  def aggregateByKey() = {

    val rdd = sparkContext.parallelize(Array((2,"aa"), (2,"bb"), (4,"c"), (4,"d")), 2)

    // aggregate算子和reduceByKey类似，其实reduceByKey是aggregate的一个特例
    //aggregate算子操作需要三个参数：对于RDD[T]
    //1.参数一为每个key的初始值  类型为 U
    //2.参数二为seq function即如何进行shuffle map-side 的本地聚合 需要类型为 (U,T) => U 函数
    //3.参数三位如何进行shuffle reduce-side 的全局聚合 需要类型为 (U,U) => U 函数
    val fun1 = (x:Int, y:String) => x + y.length
    val fun2 = (x:Int, y:Int) => x+y
    // 本例 将key一样的String进行字符个数统计
    rdd.aggregateByKey(0)(fun1, fun2).foreach{
      x =>
        println(x._1, x._2)
    }

  }

  @Test
  def sortByKey() = {
    val rdd = sparkContext.parallelize(Array((1, "a"), (2, "b"), (3, "c"), (4, "d")), 1)

    rdd.sortByKey().foreach(println)

//    rdd.sortBy(x => if (x._1 % 2 == 0) 0 else 1).foreach(println)
  }

  @Test
  def join() = {
    val rdd = sparkContext.parallelize(Array((1, "a"), (2, "b"), (3, "c"), (4, "d")), 1)
    val rdd1 = sparkContext.parallelize(Array((1, 1), (2, 2), (5, "d")), 1)

    // 返回：
    // (1,(a,1))
    // (2,(b,2))
    rdd.join(rdd1).foreach(println)

    // 返回：
    // (1,(a,Some(1)))
    // (2,(b,Some(2)))
    // (3,(c,None))
    // (4,(d,None))
    rdd.leftOuterJoin(rdd1).foreach(println)

    // 返回：
    // (1,(Some(a),1))
    // (5,(None,d))
    // (2,(Some(b),2))
    rdd.rightOuterJoin(rdd1).foreach(println)

  }

  @Test
  def cogroup() = {
    val rdd = sparkContext.parallelize(Array((1, "a"), (2, "b"), (3, "c"), (4, "d")), 1)
    val rdd1 = sparkContext.parallelize(Array((1, 1), (2, 2), (5, "d")), 1)

    // 返回：
    // (1,(List(a),List(1)))
    // (2,(List(b),List(2)))
    // (3,(List(c),List()))
    // (4,(List(d),List()))
    // (5,(List(),List(d)))

    rdd.cogroup(rdd1).foreach(x => println(x._1, (x._2._1.toList, x._2._2.toList)))

  }

  @Test
  def cartesian() = {
    val rdd = sparkContext.parallelize(Array(1,2), 1)
    val rdd1 = sparkContext.parallelize(Array("a","b","c"), 1)

    // 返回
    // (1,a)
    // (1,b)
    // (1,c)
    // (2,a)
    // (2,b)
    // (2,c)
    rdd.cartesian(rdd1).foreach(println)


  }

  @Test
  def coalesce() = {
    val rdd = sparkContext.parallelize(Array(1,2,3,4,5,6,7,8,9,10), 5)

    /**
      * coalesce算子是一般是将RDD的partition数量缩减，将一定的数据压缩到更少的partition分区中
      * 使用场景：
      * 一般在filter算子之后，数据量减少产生部分倾斜，会使用coalesce算子进行优化
      * coalesce算子会让数据更加紧凑。
      * 注意：是减少数据倾斜而不是消除
      *
      * 当shuffle=true时，可以将partition数量减少到很小（避免partition分布在多机的情况下）
      * 也可以将partition的数目由原先的增大，将会使用hash partitioner
      */
    val cnt = rdd.coalesce(1).mapPartitionsWithIndex{
      (index, its) =>
        val list = its.toList
        println("partition{} " + index + " data{} " + list)
        list.iterator
    }.count()

  }

  @Test
  def repartition() = {
    val rdd = sparkContext.parallelize(Array(1,2,3,4,5,6,7,8,9,10), 5)

    /**
      * repartition算子，用于任意将RDD的partition增多或减少
      * coalesce算子往往是将RRD的partition减少（其实repartition算子是调用coalesce(shuffle = true)）
      * 使用场景：
      * 有时候自动配置的partition数目过于少，为了进行优化可以增加partition数目提高并行度
      * 一个经典的例子：Spark SQL从hive查询数据，会根据hive对应的hdfs文件的block数目决定加载
      * 出来的RDD的partition数量，这里默认的partition数目是无法设置的但可以repartition
      */
    val cnt = rdd.repartition(3).mapPartitionsWithIndex{
      (index, its) =>
        val list = its.toList
        println("partition{} " + index + " data{} " + list)
        list.iterator
    }.count()

  }

}
