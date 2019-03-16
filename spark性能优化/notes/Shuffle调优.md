Spark Shuffle 相关调优
------
大多数Spark作业的性能主要就是消耗在了shuffle环节，因为该环节包含了大量的磁盘IO、序列化、网络数据传输等操作。
因此，很有必要对shuffle过程进行调优。但是也必须提醒的是，影响一个Spark作业性能的因素，主要还是代码开发、资源参数以及数据倾斜，
shuffle调优只能在整个Spark的性能调优中占到一小部分而已。

在进行shuffle的时候，必须将各个节点上相同的key拉取到某个节点上的一个task来进行处理，比如按照key进行聚合或join等操作。
此时如果某个key对应的数据量特别大的话，就会发生数据倾斜。数据倾斜只会发生在shuffle过程中。
常用的并且可能会触发shuffle操作的算子有：distinct、groupByKey、reduceByKey、aggregateByKey、join、cogroup、repartition等。

shuffle调优分为两种,一种是代码开发调优,一种是shuffle参数根据实际情况调优,关键还是代码开发。

首先代码开发层面，要减少shuffle的开销，主要有两个思路：

1. 减少shuffle次数，尽量不改变key，把数据处理在local完成;
2. 减少shuffle的数据规模;

## 先去重再合并
比如有A、B这样两个规模比较大的RDD，如果各自内部有大量重复，那么可以先各自去重，再合并，
这样可以大幅度减小shuffle的开销。
```scala
A.union(B).distinct()
// 优化后的代码：
A.distinct().union(B.distinct()).distinct()
```
类似地，还有先filter等操作，目的也是要先对大的RDD进行“瘦身”操作，然后在做其他操作。
## broadcast代替join
这种优化是一种特定场景使用，就是一个大的RDD A去join一个小的RDD B，类似hive的map join。
比如rdd A是订单信息超大，结构为(id, date, pid),rdd B商品信息很小，结构为(pid, pname)
现在需要取出所有订单的商品名称：
```scala
A.map{case (id, date, pid) => (pid, (id, date))}
 .join(B)
 .map{case (pid, ((id, date), pname)) => (id, date, pname)}
 
// 优化后的代码
val b = sc.broadcast(B.collectAsMap)
A.map{
    case (id, date, pid) =>
    val map = B.value.toMap
    val pname = map.get(pid)
    (id, date, pname)
}
 ```
[broadcast 示例代码：ShuffleOptimize.scala](../src/main/scala/org/spark/notes/ShuffleOptimize.scala)

## 数据倾斜的shuffle
有这样的场景，一个非常巨大的RDD A，结构是(countryId, product)，key是国家id，value是商品的具体信息。
当shuffle的时候，是根据key进行hash算法来选择节点的，但是事实上这个countryId的分布是极其不均匀的，
大部分商品都在美国（countryId=1），其中一台slave的CPU特别高，计算全部聚集到那一台去了。

解决方法：改进一下key，让它的shuffle能够均匀分布（比如可以拿countryId+商品名称的tuple作key，甚至可以加上一个随机串）

## reduceByKey代替groupByKey
reduceByKey会在当前节点（local）中做reduce操作，也就是说，会在shuffle前，会尽可能地减小数据量。
而groupByKey则不是，它会不做任何处理而直接去shuffle。当然，有一些场景下，功能上二者并不能互相替换，
因为reduceByKey要求参与运算的value，并且和输出的value类型要一样，但是groupByKey则没有这个要求。

## treeReduce来代替reduce
主要用于单个reduce操作开销比较大，reduce是一次性聚合，shuffle量太大，而treeReduce是分批聚合，
更为保险，可以通过treeReduce的深度来控制每次reduce的规模。

## 提高shuffle操作的并行度
如果我们必须要对数据倾斜迎难而上，那么建议优先使用这种方案，因为这是处理数据倾斜最简单的一种方案。
在对RDD执行shuffle算子时，给shuffle算子传入一个参数，比如reduceByKey(1000)，该参数就设置了
这个shuffle算子执行时shuffle read task的数量。对于Spark SQL中的shuffle类语句，比如group by、join等，
需要设置一个参数，即spark.sql.shuffle.partitions，该参数代表了shuffle read task的并行度，该值默认是200，
对于很多场景来说都有点过小。

## 参数调优
以下是Shffule过程中的一些主要参数，基于实践经验给出调优建议：
### spark.shuffle.file.buffer
* 默认值：32k
* 参数说明：该参数用于设置shuffle write task的BufferedOutputStream的buffer缓冲大小。
将数据写到磁盘文件之前，会先写入buffer缓冲中，待缓冲写满之后，才会溢写到磁盘。
* 调优建议：如果作业可用的内存资源较为充足的话，可以适当增加这个参数的大小（比如64k），
从而减少shuffle write过程中溢写磁盘文件的次数，也就可以减少磁盘IO次数，进而提升性能。
在实践中发现，合理调节该参数，性能会有1%~5%的提升。

### spark.reducer.maxSizeInFlight
* 默认值：48m
* 参数说明：该参数用于设置shuffle read task的buffer缓冲大小，而这个buffer缓冲决定了每次能够拉取多少数据。
* 调优建议：如果作业可用的内存资源较为充足的话，可以适当增加这个参数的大小（比如96m），
从而减少拉取数据的次数，也就可以减少网络传输的次数，进而提升性能。在实践中发现，合理调节该参数，性能会有1%~5%的提升。

### spark.shuffle.io.maxRetries
* 默认值：3
* 参数说明：shuffle read task从shuffle write task所在节点拉取属于自己的数据时，如果因为网络异常导致拉取失败，
是会自动进行重试的。该参数就代表了可以重试的最大次数。如果在指定次数之内拉取 还是没有成功，就可能会导致作业执行失败。
* 调优建议：对于那些包含了特别耗时的shuffle操作的作业，建议增加重试最大次数（比如60次），
以避免由于JVM的full gc或者网络不稳定等因素导致的数据拉取失败。在实践中发现，对于针对超大数据量（数十亿~上百亿）的shuffle过程，
调节该参数可以大幅度提升稳定性。

### spark.shuffle.io.retryWait
* 默认值：5s
* 参数说明：具体解释同上，该参数代表了每次重试拉取数据的等待间隔，默认是5s。
* 调优建议：建议加大间隔时长（比如60s），以增加shuffle操作的稳定性。

### spark.shuffle.memoryFraction
* 默认值：0.2
* 参数说明：该参数代表了Executor内存中，分配给shuffle read task进行聚合操作的内存比例，默认是20%。
* 调优建议：如果内存充足，而且很少使用持久化操作，建议调高这个比例，给shuffle read的聚合操作更多内存，
以避免由于内存不足导致聚合过程中频繁读写磁盘。在实践中发现，合理调节该参数可以将性能提升10%左右。

### spark.shuffle.manager
* 默认值：sort
* 参数说明：该参数用于设置ShuffleManager的类型。Spark 1.5以后，有三个可选项：hash、sort和tungsten-sort。
HashShuffleManager是Spark 1.1以前的默认选项，但是Spark 1.1以及之后的版本默认都是SortShuffleManager了。
tungsten-sort与sort类似，但是使用了tungsten计划中的 堆外内存管理机制，内存使用效率更高。
* 调优建议：由于SortShuffleManager默认会对数据进行排序，因此如果你的业务逻辑中需要该排序机制的话，
则使用默认的SortShuffleManager就可以；而如果你的业务逻辑不需要对数据进行排序，那么建议参考后面的几个参数调优，
通过bypass机制或优化的 HashShuffleManager来避免排序操作，同时提供较好的磁盘读写性能。这里要注意的是，
tungsten-sort要慎用，因为之前发现了一些相应的bug。
### spark.shuffle.sort.bypassMergeThreshold
* 默认值：200
* 参数说明：当ShuffleManager为SortShuffleManager时，如果shuffle read task的数量小于这个阈值（默认是200），
则shuffle write过程中不会进行排序操作，而是直接按照未经优化的HashShuffleManager的方式去写数据，
但是最后会将每个task产生的所有临时磁盘文件都合并成一个文件，并会创建单独的索引文件。
* 调优建议：当你使用SortShuffleManager时，如果的确不需要排序操作，那么建议将这个参数调大一些，大于shuffle read task的数量。
那么此时就会自动启用bypass机制，map-side就不会进行排序了，减少了排序的性能开销。但是这种方式下，依然会产生大量的磁盘文件，因此shuffle write性能有待提高。
### spark.shuffle.consolidateFiles
* 默认值：false
* 参数说明：如果使用HashShuffleManager，该参数有效。如果设置为true，那么就会开启consolidate机制，会大幅度合并shuffle write的输出文件，
对于shuffle read task数量特别多的情况下，这种方法可以极大地减少磁盘IO开销，提升性能。
* 调优建议：如果的确不需要SortShuffleManager的排序机制，那么除了使用bypass机制，还可以尝试将spark.shffle.manager参数手动指定为hash，
使用HashShuffleManager，同时开启consolidate机制。在实践中尝试过，发现其性能比开启了bypass机制的SortShuffleManager要高出10%~30%。







 


 