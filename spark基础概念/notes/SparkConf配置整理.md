SparkConf解析和常用配置说明
-------
* [Contents](#Contents)
	* [SparkConf](#SparkConf)
    * [需要关注的配置项](#需要关注的配置项) 
        * [应用程序属性](#应用程序属性)
		* [运行环境配置](#运行环境配置)
		* [Shuffle行为相关配置](#Shuffle行为相关配置)
		* [压缩和序列化配置](#压缩和序列化配置)
		* [运行时参数配置](#运行时参数配置)
		* [调度相关属性](#调度相关属性)
        * [SparkStreaming配置](#SparkStreaming配置)
        * [Spark On YARN配置](#Spark-On-YARN配置)
		* [SparkUI配置](#SparkUI配置)
## SparkConf
spark 配置可分为三层：spark properties、environment variables、还有logging配置
spark properties是由用户自己设置的，在任务中可以通过 SparkConf 类进行设置:

```scala
val conf = new SparkConf()  
             .setMaster("local")  
             .setAppName("SparkTest")  
             .set("spark.executor.memory", "1g")
```
也可以通过提交命令设置，这个时候 SparkConf 对象就不需要设置相关配置： 
```
./bin/spark-submit --name "My app" --master local[2] --executor-memory 1g --conf spark.shuffle.spill=false   
  --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps" myApp.jar 
```
spark-submit 还会加载 conf/spark-defaults.conf 中的配置

注：建议将master、appName、spark.executor.memory等配置在spark-submit提交任务时进行设置，不建议在代码里写死

sparkConf类可以为spark程序设置配置，当然这些皆可在spark-default.conf进行配置。sparkConf通过set方法将key-value形式创建一系列的spark参数。
通过new sparkconf()对象的时候，它会加载任何以’spark.*’的值，
还可以在你的程序中设置java相关的参数。而你自己设置的spark相关的参数会优先覆盖系统默认的一些参数。
一旦sparkConf对象传给spark，配置只能复制而不能被修改，spark程序不支持运行时修改参数。

我们可以通过下面的UI查看已经配置的配置项：

http://Spark-master:ui-port/history/application-id/environment/ 

## 需要关注的配置项

以spark2.2.0为例，详细可参看[官网配置](http://spark.apache.org/docs/2.2.0/configuration.html#application-properties)

### 应用程序属性
<table class="table table-bordered table-striped table-condensed">
    <tr>
        <th bgcolor="#4c4c4c">属性名称</th>
        <th bgcolor="#4c4c4c">默认值</th>
        <th bgcolor="#4c4c4c">含义</th>
    </tr>
    <tr>
        <td><font color="#a52a2a">spark.app.name</td>
	    <td>(none)</td>
        <td>应用程序的名字。这将在UI和日志数据中出现</td>
    </tr>
    <tr>
        <td><font color="#a52a2a">spark.driver.cores</td>
	    <td>1</td>
        <td>driver程序运行需要的cpu内核数</td>
    </tr>
    <tr>
        <td>spark.driver.maxResultSize</td>
	    <td>1g</td>
        <td>每个Spark action(如collect)所有分区的序列化结果的总大小限制。设置的值应该不小于1m，<br>
        0代表没有限制。如果总大小超过这个限制，程序将会终止。大的限制值可能导致driver出现内存溢出错误<br>
        （依赖于spark.driver.memory和JVM中对象的内存消耗）。</td>
    </tr>
    <tr>
        <td><font color="#a52a2a">spark.driver.memory</td>
	    <td>1g</td>
        <td>driver进程使用的内存数</td>
    </tr>
    <tr>
        <td><font color="#a52a2a">spark.executor.cores</td>
	    <td>1 in YARN mode, all the available cores on the worker in standalone and Mesos coarse-grained modes</td>
        <td>每个执行程序使用的核数。在独立和Mesos粗粒度模式下，设置此参数允许应用程序在同一个worker上运行多个执行程序，<br>
        前提是该worker上有足够的内核。否则，每个应用程序只会运行一个执行程序。</td>
    </tr>
    <tr>
        <td><font color="#a52a2a">spark.executor.memory</td>
	    <td>1g</td>
        <td>每个executor进程使用的内存数。和JVM内存串拥有相同的格式（如2g,8g)</td>
    </tr>
    <tr>
        <td>spark.extraListeners</td>
        <td>(none)</td>
        <td>注册监听器，需要实现SparkListener</td>
    </tr>
    <tr>
        <td>spark.local.dir</td>
        <td>/tmp</td>
        <td>用于Spark中“临时”空间的目录，包括存储在磁盘上的映射输出文件和RDD。<br>
            这应该位于系统中的快速本地磁盘上。它也可以是不同磁盘上多个目录的逗号分隔列表。<br>
            注意：在Spark 1.0及更高版本中，这将由集群管理器设置的SPARK_LOCAL_DIRS（Standalone，Mesos）<br>
            或LOCAL_DIRS（YARN）环境变量覆盖。</td>
    </tr>
    <tr>
        <td>spark.logConf</td>
        <td>false</td>
        <td>当SparkContext启动时，将有效的SparkConf记录为INFO</td>
    </tr>
    <tr>
        <td><font color="#a52a2a">spark.master</td>
        <td>(none)</td>
        <td>要连接的集群管理器。如（local、local[2]、yarn等）</td>
    </tr>
    <tr>
        <td><font color="#a52a2a">spark.submit.deployMode</td>
        <td>(none)</td>
        <td>Spark驱动程序的部署模式（ "client" or "cluster"）<br>
        意味着在集群内的一个节点上本地（“客户端”）或远程（“集群”）启动驱动程序。</td>
    </tr>
</table>

### 运行环境配置
<table class="table table-bordered table-striped table-condensed">
    <tr>
        <th bgcolor="#4c4c4c">属性名称</th>
        <th bgcolor="#4c4c4c">默认值</th>
        <th bgcolor="#4c4c4c">含义</th>
    </tr>
    <tr>
        <td>spark.driver.extraClassPath</td>
	    <td>(none)</td>
        <td>附加到driver的classpath的额外的classpath实体。</td>
    </tr>
    <tr>
        <td><font color="#a52a2a">spark.driver.extraJavaOptions</td>
	    <td>(none)</td>
        <td>传递给driver的JVM选项字符串。例如GC设置或者其它日志设置。<br>
        注意，在这个选项中设置Spark属性或者堆大小是不合法的。可以使用群集模式下的<br>
        spark.driver.memory和客户端模式中的--driver-memory命令行选项设置最大堆大小</td>
    </tr>
    <tr>
        <td>spark.driver.extraLibraryPath</td>
	    <td>(none)</td>
        <td>指定启动driver的JVM时用到的库路径</td>
    </tr>
    <tr>
        <td><font color="#a52a2a">spark.executor.extraJavaOptions</td>
	    <td>(none)</td>
        <td>传递给executors的JVM选项字符串。例如GC设置或者其它日志设置。注意，<br>
           在这个选项中设置Spark属性或者堆大小是不合法的。Spark属性需要用SparkConf对象<br>
           或者spark-submit脚本用到的spark-defaults.conf文件设置。堆内存可以通过<br>
           spark.executor.memory设置</td>
    </tr>
    <tr>
        <td>spark.executor.extraLibraryPath</td>
	    <td>(none)</td>
        <td>指定启动executor的JVM时用到的库路径</td>
    </tr>
    <tr>
        <td><font color="#a52a2a">spark.python.worker.memory</td>
        <td>512m</td>
        <td>在聚合期间，每个python worker进程使用的内存数。在聚合期间，如果内存<br>
            超过了这个限制，它将会将数据塞进磁盘中</td>
    </tr>
    <tr>
        <td>spark.python.profile</td>
        <td>false</td>
        <td>在Python worker中开启profiling。通过sc.show_profiles()展示分析结果。<br>
        或者在driver退出前展示分析结果。可以通过sc.dump_profiles(path)将结果dump到磁盘中。<br>
        如果一些分析结果已经手动展示，那么在driver退出前，它们再不会自动展示</td>
    </tr>
    <tr>
        <td>spark.python.profile.dump</td>
        <td>(none)</td>
        <td>driver退出前保存分析结果的dump文件的目录。每个RDD都会分别dump一个文件。<br>
        可以通过ptats.Stats()加载这些文件。如果指定了这个属性，分析结果不会自动展示</td>
    </tr>
    <tr>
        <td><font color="#a52a2a">spark.python.worker.reuse</td>
        <td>true</td>
        <td>是否重用python worker。如果是，它将使用固定数量的Python workers，<br>
        而不需要为每个任务fork()一个Python进程。如果有一个非常大的广播，这个设置将非常有用。<br>
        因为，广播不需要为每个任务从JVM到Python worker传递一次</td>
    </tr>
</table>

### Shuffle行为相关配置
<table class="table table-bordered table-striped table-condensed">
    <tr>
        <th bgcolor="#4c4c4c">属性名称</th>
        <th bgcolor="#4c4c4c">默认值</th>
        <th bgcolor="#4c4c4c">含义</th>
    </tr>
    <tr>
        <td>spark.reducer.maxSizeInFlight</td>
	    <td>48m</td>
        <td>从每个reduce任务同时获取的map输出的最大大小。由于每个输出都需要我们创建一个缓冲区来接收它，<br>
            这表示每个reduce任务的固定内存开销，所以除非你有大量内存，否则保持小。</td>
    </tr>
    <tr>
        <td><font color="#a52a2a">spark.shuffle.compress</td>
	    <td>true</td>
        <td>是否压缩map操作的输出文件。一般情况下，true是一个好的选择。</td>
    </tr>
    <tr>
        <td>spark.shuffle.file.buffer</td>
	    <td>32k</td>
        <td>每个shuffle文件输出流的内存缓冲区的大小。<br>
            这些缓冲区减少了在创建中间shuffle文件时进行的磁盘搜索和系统调用的次数。</td>
    </tr>
    <tr>
        <td><font color="#a52a2a">spark.shuffle.sort.bypassMergeThreshold</td>
	    <td>200</td>
        <td>（高级）在基于排序的shuffle管理器中，如果没有map端聚合，设置最多有多个reduce分区，避免合并排序数据</td>
    </tr>
    <tr>
        <td>spark.shuffle.spill.compress</td>
	    <td>true</td>
        <td>在shuffle时，是否将spilling的数据压缩。压缩算法通过spark.io.compression.codec指定。</td>
    </tr>
</table>

### 压缩和序列化配置
<table class="table table-bordered table-striped table-condensed">
    <tr>
        <th bgcolor="#4c4c4c">属性名称</th>
        <th bgcolor="#4c4c4c">默认值</th>
        <th bgcolor="#4c4c4c">含义</th>
    </tr>
    <tr>
        <td>spark.broadcast.compress</td>
	    <td>true</td>
        <td>在发送广播变量之前是否压缩它</td>
    </tr>
    <tr>
        <td><font color="#a52a2a">spark.io.compression.codec</td>
	    <td>lz4</td>
        <td>用于压缩内部数据的编解码器，例如RDD分区，事件日志，广播变量和随机输出。<br>
            默认情况下，Spark提供三种编解码器：lz4，lzf和snappy。<br>
            您还可以使用完全限定的类名来指定编解码器，例如org.apache.spark.io.LZ4CompressionCodec，<br>
            org.apache.spark.io.LZFCompressionCodec和org.apache.spark.io.SnappyCompressionCodec.</td>
    </tr>
    <tr>
        <td>spark.io.compression.lz4.blockSize</td>
	    <td>32k</td>
        <td>在使用LZ4压缩编解码器的情况下，LZ4压缩中使用的块大小。<br>
            当使用LZ4时，降低此块大小也会降低shuffle内存使用量。</td>
    </tr>
    <tr>
        <td>spark.io.compression.snappy.blockSize</td>
	    <td>32k</td>
        <td>在使用Snappy压缩编解码器的情况下，在Snappy压缩中使用的块大小。<br>
            当使用Snappy时，降低此块大小也会降低shuffle内存使用量。</td>
    </tr>
    <tr>
        <td>spark.kryo.classesToRegister</td>
	    <td>(none)</td>
        <td>如果你用Kryo序列化，给定的用逗号分隔的自定义类名列表表示要注册的类</td>
    </tr>
    <tr>
        <td>spark.kryo.referenceTracking</td>
	    <td>true</td>
        <td>当用Kryo序列化时，跟踪是否引用同一对象。如果你的对象图有环，这是必须的设置。<br>
            如果他们包含相同对象的多个副本，这个设置对效率是有用的。如果你知道不在这两个场景，那么可以禁用它以提高效率</td>
    </tr>
    <tr>
        <td>spark.kryo.registrationRequired</td>
	    <td>false</td>
        <td>是否需要注册为Kyro可用。如果设置为true，然后一个没有注册的类被序列化，则Kyro会抛出异常。<br>
        如果设置为false，Kryo将会同时写每个对象和其非注册类名。写类名可能造成显著地性能瓶颈。</td>
    </tr>
    <tr>
        <td>spark.kryo.registrator</td>
	    <td>(none)</td>
        <td>如果您使用Kryo序列化，请提供以逗号分隔的类列表，这些类使用Kryo注册您的自定义类。<br>
            如果您需要以自定义方式注册类，则此属性非常有用</td>
    </tr>
    <tr>
        <td>spark.kryoserializer.buffer.max</td>
	    <td>64m</td>
        <td>Kryo序列化缓存允许的最大值。这个值必须大于你尝试序列化的对象</td>
    </tr>
    <tr>
        <td>spark.kryoserializer.buffer</td>
	    <td>64k</td>
        <td>Kyro序列化缓存的大小。这样worker上的每个核都有一个缓存。如果有需要，<br>
        缓存会涨到spark.kryoserializer.buffer.max设置的值那么大。</td>
    </tr>
    <tr>
        <td>spark.rdd.compress</td>
	    <td>false</td>
        <td>是否压缩序列化的RDD分区。花费一些额外的CPU时间的同时节省大量的空间</td>
    </tr>
    <tr>
        <td>spark.serializer</td>
	    <td>org.apache.spark.serializer.JavaSerializer</td>
        <td>序列化对象使用的类。默认的Java序列化类可以序列化任何可序列化的java对象但是它很慢。<br>
        所以我们建议使用org.apache.spark.serializer.KryoSerializer并在需要速度时配置Kryo序列化。<br>
        可以是org.apache.spark.Serializer的任何子类</td>
    </tr>
</table>

### 运行时参数配置
<table class="table table-bordered table-striped table-condensed">
    <tr>
        <th bgcolor="#4c4c4c">属性名称</th>
        <th bgcolor="#4c4c4c">默认值</th>
        <th bgcolor="#4c4c4c">含义</th>
    </tr>
    <tr>
        <td>spark.broadcast.blockSize</td>
	    <td>4m</td>
        <td>TorrentBroadcastFactory传输的块大小，太大值会降低并发，太小的值会出现性能瓶颈</td>
    </tr>
    <tr>
        <td><font color="#a52a2a">spark.default.parallelism</td>
	    <td>对于像reduceByKey和join这样的分布式shuffle操作，父RDD中的最多分区数量<br>
	    对于没有父RDD shuffle的操作，它取决于集群管理器：<br>
	    1. 本地模式：本地计算机上的核数 <br>
        2. Mesos细粒度模式：8 <br>
        3. 其他：所有执行程序节点上的核数总数或2，以较大者为准
        </td>
        <td>如果用户不设置，系统使用集群中运行shuffle操作的默认任务数（groupByKey、 reduceByKey等）</td>
    </tr>
    <tr>
        <td>spark.files.useFetchCache</td>
	    <td>true</td>
        <td>如果设置为true（默认值），则文件获取将使用由属于同一应用程序的执行程序共享的本地缓存，这可以在同一主机上运行多个执行程序时提高任务启动性能。<br>
            如果设置为false，则将禁用这些缓存优化，并且所有执行程序都将获取自己的文件副本。</td>
    </tr>
</table>

### 调度相关属性
<table class="table table-bordered table-striped table-condensed">
    <tr>
        <th bgcolor="#4c4c4c">属性名称</th>
        <th bgcolor="#4c4c4c">默认值</th>
        <th bgcolor="#4c4c4c">含义</th>
    </tr>
    <tr>
        <td>spark.task.cpus</td>
	    <td>1</td>
        <td>为每个任务分配的内核数</td>
    </tr>
    <tr>
        <td>spark.task.maxFailures</td>
	    <td>4</td>
        <td>Task失败的最大重试次数</td>
    </tr>
    <tr>
        <td><font color="#a52a2a">spark.scheduler.mode</td>
	    <td>FIFO</td>
        <td>Spark的任务调度模式，还有一种Fair模式</td>
    </tr>
    <tr>
        <td><font color="#a52a2a">spark.speculation</td>
	    <td>false</td>
        <td>设置为“true”，开启任务的推测执行。<br>
            这意味着如果一个或多个任务在一个阶段中运行缓慢，它们将被重新启动</td>
    </tr>
    <tr>
        <td>spark.speculation.interval</td>
	    <td>100ms</td>
        <td>Spark会多久检查一下要推测的任务。</td>
    </tr>
    <tr>
        <td><font color="#a52a2a">spark.locality.wait</td>
	    <td>3s</td>
        <td>本参数是以毫秒为单位启动本地数据task的等待时间，如果超出就启动下一本地优先级别的task。<br>
        该设置同样可以应用到各优先级别的本地性之间（本地进程 -> 本地节点 -> 本地机架 -> 任意节点 ），<br>
        当然，也可以通过spark.locality.wait.node等参数设置不同优先级别的本地性</td>
    </tr>
    <tr>
        <td>spark.locality.wait.process</td>
	    <td>spark.locality.wait</td>
        <td>本地进程级别的本地等待时间</td>
    </tr>
    <tr>
        <td>spark.locality.wait.node</td>
	    <td>spark.locality.wait</td>
        <td>本地节点级别的本地等待时间</td>
    </tr>
    <tr>
        <td>spark.locality.wait.rack</td>
	    <td>spark.locality.wait</td>
        <td>本地机架级别的本地等待时间</td>
    </tr>
<tr>
        <td>spark.scheduler.revive.interval</td>
	    <td>1s</td>
        <td>复活重新获取资源的Task的最长时间间隔，发生在Task因为本地资源不足<br>
            而将资源分配给其他Task运行后进入等待时间，如果这个等待时间内重新获取足够的资源就继续计算</td>
    </tr>
</table>

### SparkStreaming配置
<table class="table table-bordered table-striped table-condensed">
    <tr>
        <th bgcolor="#4c4c4c">属性名称</th>
        <th bgcolor="#4c4c4c">默认值</th>
        <th bgcolor="#4c4c4c">含义</th>
    </tr>
    <tr>
        <td>spark.streaming.blockInterval</td>
	    <td>200ms</td>
        <td>Spark Streaming接收器接收的数据在存储到Spark之前被分块为数据块的时间间隔。建议低值：50毫秒</td>
    </tr>
    <tr>
        <td><font color="#a52a2a">spark.streaming.receiver.writeAheadLog.enable</td>
	    <td>false</td>
        <td>是否启用接收器的预写日志。通过接收器接收的所有输入数据将被保存到提前写入日志，以便在驱动程序失败后恢复<br>
        能提高容错性，但对性能有影响</td>
    </tr>
    <tr>
        <td>spark.streaming.unpersist</td>
	    <td>true</td>
        <td>强制将通过Spark Streaming生成并持久化的RDD自动从Spark内存中非持久化。通过Spark Streaming接收的原始输入数据也将清除。<br>
        设置这个属性为false允许流应用程序访问原始数据和持久化RDD，因为它们没有被自动清除，但是它会造成更高的内存花费</td>
    </tr>
</table>

### Spark On YARN配置
<table class="table table-bordered table-striped table-condensed">
    <tr>
        <th bgcolor="#4c4c4c">属性名称</th>
        <th bgcolor="#4c4c4c">默认值</th>
        <th bgcolor="#4c4c4c">含义</th>
    </tr>
    <tr>
        <td>spark.yarn.am.memory</td>
	    <td>512m</td>
        <td>在客户端模式下用于YARN Application Master的内存量，格式与JVM内存字符串相同（例如512m，2g）。<br>
            在群集模式下，请改用spark.driver.memory</td>
    </tr>
    <tr>
        <td>spark.yarn.am.cores</td>
	    <td>1</td>
        <td>在客户端模式下用于YARN Application Master的核数。<br>
            在群集模式下，请改用spark.driver.cores。</td>
    </tr>
    <tr>
        <td>spark.yarn.am.waitTime</td>
	    <td>100s</td>
        <td>在集群模式下，YARN Application Master等待SparkContext初始化的时间。<br>
            在客户端模式下，YARN Application Master等待驱动程序连接到它的时间</td>
    </tr>
    <tr>
        <td>spark.yarn.preserve.staging.files</td>
	    <td>false</td>
        <td>设置为true以在作业结束时保留暂存文件（Spark jar，app jar，分布式缓存文件），而不是删除它们</td>
    </tr>
    <tr>
        <td>spark.executor.instances</td>
	    <td>2</td>
        <td>静态分配的执行程序数。<br>
            使用spark.dynamicAllocation.enabled，初始执行程序集至少会是这么大。</td>
    </tr>
    <tr>
        <td>spark.yarn.jars</td>
	    <td>(none)</td>
        <td>包含要分发到YARN容器的Spark代码的库列表。<br>
            默认情况下，YARN上的Spark将使用本地安装的Spark jar，但Spark jar也可以位于HDFS上的世界可读位置。<br>
            这允许YARN将其缓存在节点上，这样每次应用程序运行时都不需要分发它。例如，要指向HDFS上的jar，请将此配置设置为hdfs:///path</td>
    </tr>
    <tr>
        <td><font color="#a52a2a">spark.yarn.archive</td>
	    <td>(none)</td>
        <td>包含所需Spark Spark的存档，以便分发到YARN缓存。<br>
            如果设置，则此配置将替换spark.yarn.jars，并且该存档将用于所有应用程序的容器中。
        </td>
    </tr>
</table>

### SparkUI配置
<table class="table table-bordered table-striped table-condensed">
    <tr>
        <th bgcolor="#4c4c4c">属性名称</th>
        <th bgcolor="#4c4c4c">默认值</th>
        <th bgcolor="#4c4c4c">含义</th>
    </tr>
    <tr>
        <td>spark.eventLog.enabled</td>
	    <td>false</td>
        <td>是否记录Spark事件，对于在应用程序完成后重建Web UI非常有用</td>
    </tr>
    <tr>
        <td>spark.eventLog.dir</td>
	    <td>file:///tmp/spark-events</td>
        <td>如果spark.eventLog.enabled为true，则记录Spark事件的基目录。<br>
            在此基本目录中，Spark为每个应用程序创建一个子目录，并将特定于该应用程序的事件记录在此目录中。<br>
            用户可能希望将其设置为统一位置（如HDFS目录），以便历史记录服务器可以读取历史记录文件</td>
    </tr>
    <tr>
        <td>spark.eventLog.compress</td>
	    <td>false</td>
        <td>是否压缩已记录的事件，如果spark.eventLog.enabled为true。压缩将使用spark.io.compression.codec</td>
    </tr>
    <tr>
        <td>spark.ui.killEnabled</td>
	    <td>true</td>
        <td>允许从Web UI中删除作业和阶段。</td>
    </tr>
</table>




