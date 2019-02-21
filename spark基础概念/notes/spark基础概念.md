# spark基础概念

1. RDD：

RDD（Resilient Distributed Dataset）弹性分布式数据集：是Spark中的抽象数据结构类型，
任何数据在Spark中都被表示为RDD。可以简单的看成看
成是一个数组，里面的数据是分区存储的。不同分区的数据可以分布在不同的机器上，
同时可以被并行处理。有如下几点概要：
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

<a href="http://spark.apache.org/docs/latest/rdd-programming-guide.html#resilient-distributed-datasets-rdds" target="_blank">RDD</a>
![RDD示例](images/RDD.png) 



   
