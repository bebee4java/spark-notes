# spark RDD解释
RDD（Resilient Distributed Dataset）弹性分布式数据集：是Spark中的抽象数据结构类型，
任何数据在Spark中都被表示为RDD。可以简单的看成看成是一个数组，里面的数据是分区存储的。
不同分区的数据可以分布在不同的机器上，同时可以被并行处理。有如下几点概要：
- RDD是一个抽象类
- RDD带泛型，支持多种数据类型：String，Long，Object
- RDD是不可变的分区集合，支持并行计算

RDD有如下5个特性：
* A list of partitions

  由一系列的分区数据组成
* A function for computing each split

  每一个分片会计算函数
* A list of dependencies on other RDDs

  rdd之间存在一系列的关系
* Optionally, a Partitioner for key-value RDDs 
    (e.g. to say that the RDD is hash-partitioned)
    
    可选的，对于kv类型的rdd会作用一个Partitioner进行分区，类似MR里
* Optionally, a list of preferred locations to compute each split on 
   (e.g. block locations for an HDFS file）
   
   可选的，数据会就近进行计算（移动计算，数据本地化）

[RDD官方解释](http://spark.apache.org/docs/latest/rdd-programming-guide.html#resilient-distributed-datasets-rdds)

![RDD示例](images/RDD.png)

RDD创建:
1. 并行化驱动程序中的现有集合 
``` 
    val data = Array(1, 2, 3, 4, 5) 
    val distData = sc.parallelize(data)
```
2. 引用外部存储系统中的数据集，例如共享文件系统，HDFS，HBase或提供Hadoop
   InputFormat的任何数据源 
```
    scala> val distFile = sc.textFile("data.txt")
    distFile: org.apache.spark.rdd.RDD[String] = data.txt MapPartitionsRDD[10] at textFile at <console>:26
```
[sparkRDD创建示例代码: SparkRDDTest.scala](src/main/scala/org/spark/notes/SparkRDDTest.scala)
   
   



   
