Spark RDD 核心算子介绍 
------
Spark RDD支持两种类型的操作： 
- transformations 转换（从现有数据集创建新的数据集，本质是RDD到RDD）
- actions 操作（在数据集上运行计算后将值返回到驱动程序，本质是RDD到result）

Spark中的所有transformations算子都是惰性的，它们不会立即计算结果。他们只记录应用于数据集上的转换操作
Actions算子则会触发Spark job，将最后结果返回到驱动程序。这种设计使Spark能够更有效地运行。

![spark rdd operator](images/rdd_operator.png)

## transformations算子
- map(func)

  将函数func作用到数据集的每一个元素上，并返回新的数据集。在T类型的RDD上运行是，func类型
  必须是 T => U
- filter(func)

  选出所有执行func返回值为true的元素，并返回新的数据集。在T类型的RDD运行是，func
  类型必须是 T => Boolean
- flatMap(func)

  将函数func作用到数据集的每一个元素上然后再扁平化，并返回新的数据集
  与map类似，但每个输入项都可以映射到0个或多个输出项因此func应该返回seq而不是单个项
  在T类型的RDD上运行是，func必须是T => Seq(U)
- mapPartitions(func) 

  将函数func作用到数据集的每一个分区上，并返回新的数据集
  与map类似，但在RDD的每个分区（块）上单独运行，因此当在T类型的RDD上运行时，
  func必须是iterator[T] => iterator[U]类型
- mapPartitionsWithIndex(func)

  与mapPartitions类似，但为func提供了表示分区索引的整数值，因此在T类型的RDD上运行时，
  func必须是类型（int，iterator[T]）=> iterator[U]
- groupBy(func)

  根据用户自定义的func进行分组，即将函数func作用到数据集的每一个元素x上，形成(finc(x),x)键值对，再进行groupByKey
  返回新的rdd类型为RDD[(K, Iterable[T])]
- groupByKey([numTasks])
  
  当调用（k，v）对的数据集时，返回（k，iterable<v>）对的数据集,将相同的key的数据分发到一起,可以指定reduce任务的数量。
  注意：如果您分组是为了对每个键执行聚合（如求和或平均值），则使用ReduceByKey或AggregateByKey将产生更好的性能。
  注意：默认情况下，输出中的并行度级别取决于父RDD的分区数。您可以传递一个可选的numtasks参数来设置不同数量的任务。
- reduceByKey(func, [numTasks])
  
  当对（k，v）对的数据集调用时，返回一个（k，v）对的数据集，其中每个键的对应的值使用给定的reduce函数func聚合，
  该函数的类型必须为（v，v）=>v。与groupbykey中一样，reduce任务的数量可以通过可选的第二个参数配置。
- aggregateByKey(zeroValue)(seqOp, combOp, [numTasks])

  当对（k，v）对的数据集调用时，返回一个（k，u）对的数据集，其中每个键的值使用给定的组合函数和初始值进行聚合。
  允许与输入值类型不同的聚合值类型
- sortByKey([ascending], [numTasks])
  
  当对（k，v）对的数据集上调用时，返回由键k按升序或降序排序的（k，v）对的数据集,默认升序
- union(otherDataset)

  将两个数据类型相同的RDD进行合并，并返回新的RDD，新RDD包含所有元素，注意不会去重,并且
  保持原有rdd的partition的个数
- join(otherDataset, [numTasks])

  当对（k，v）和（k，w）类型的数据集调用时，返回一个（k，（v，w））对的数据集。
  外部联接通过leftouterjoin、rightouterjoin和fulloterjoin支持
- cartesian(otherDataset)

  当调用t和u类型的数据集时，返回一个（t，u）对（所有元素对）的数据集。笛卡尔积。
- cogroup(otherDataset, [numTasks])
  
  当调用类型（k，v）和（k，w）的数据集时，返回（k，（iterable<v>，iterable<w>）元组的数据集。
  所有k都会有元素，此操作也称为GroupWith
- coalesce(numPartitions)

  将RDD中的分区数减少到numPartitions。对于过滤大型数据集后更高效地运行操作很有用
- repartition(numPartitions)
  
  随机重组RDD中的数据，以创建更多或更少的分区，并在分区之间进行平衡。这总是在网络上shuffle所有数据
- repartitionAndSortWithinPartitions(partitioner)

  根据给定的分区器对RDD重新分区，并在每个生成的分区内，按键对记录进行排序。这比调用重新分区然后
  在每个分区内进行排序更有效，因为它可以将排序向下推送到shuffle机器上。
- intersection(otherDataset)
  
  取两个数据类型相同的RDD的交集元素，并返回新的RDD，注意相同的元素会去重
- distinct([numTasks]))
  
  返回包含源数据集的不同元素的新数据集
  
- sample(withReplacement, fraction, seed)

  对RDD元素进行采样取数，返回新的RDD。withReplacement：取出的元素是否放回
  fraction：抽取比例 seed：种子如果写死每次抽样相同







