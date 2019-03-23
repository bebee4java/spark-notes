SparkSQL用户自定义UDF、UDAF函数
------
* [Contents](#Contents)
	* [UDF](#UDF)
	* [UDAF](#UDAF)
	
UDF：用户自定义函数(User Defined Function)。一行输入一行输出。
UDAF：用户自定义聚合函数(User Defined Aggregate Function)。多行输入一行输出。
UDTF：用户自定义表函数(User Defined Table Generating Function)。一行输入多行输出。如hive/spark中的explode、json_tuple函数

## UDF
UDF(User-Defined-Function)，也就是最基本的函数，它提供了SQL中对字段转换的功能，
不涉及聚合操作。例如将日期类型转换成字符串类型，格式化字段。对表中的单行进行转换，
以便为每行生成单个对应的输出值。

创建udf函数可以通过spark.udf.register("funcName", func) 来进行注册。然后直接
使用sql：select funcName(name) from people 来查询。

或者，创建UserDefinedFunction，通过udf(func),需要引入import org.apache.spark.sql.functions._
，然后使用df.select(func(df("id")))。

[Spark SQL UDF示例代码: SparkSQLUDF.scala](../src/main/scala/org/spark/notes/SparkSQLUDF.scala)

## UDAF
UDAF(User-Defined-Aggregate-Function)函数是用户自定义的聚合函数，为Spark SQL
提供对数据集的聚合功能，类似于max()、min()、count()等功能，只不过自定义的功能是
根据具体的业务功能来确定的。因为DataFrame是弱类型的，DataSet是强类型，所以自定义的
UDAF也提供了两种实现，一个是弱类型的一个是强类型的(不常用)。

1. 弱类型用法

    需要继承UserDefindAggregateFunction，实现它的方法:
    ```scala
    object MyAverage extends UserDefinedAggregateFunction {
      // Data types of input arguments of this aggregate function
      def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)
      // Data types of values in the aggregation buffer
      def bufferSchema: StructType = {
        StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
      }
      // The data type of the returned value
      def dataType: DataType = DoubleType
      // Whether this function always returns the same output on the identical input
      def deterministic: Boolean = true
      // Initializes the given aggregation buffer. The buffer itself is a `Row` that in addition to
      // standard methods like retrieving a value at an index (e.g., get(), getBoolean()), provides
      // the opportunity to update its values. Note that arrays and maps inside the buffer are still
      // immutable.
      def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = 0L
        buffer(1) = 0L
      }
      // Updates the given aggregation buffer `buffer` with new input data from `input`
      def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        if (!input.isNullAt(0)) {
          buffer(0) = buffer.getLong(0) + input.getLong(0)
          buffer(1) = buffer.getLong(1) + 1
        }
      }
      // Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
      def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
        buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
      }
      // Calculates the final result
      def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)
    }
    ```
2. 强类型用法

    需要继承Aggregate,实现它的方法。既然是强类型，那么其中肯定涉及到对象的存在:
    ```scala
    case class Employee(name: String, salary: Long)
    case class Average(var sum: Long, var count: Long)
    
    object MyAverage extends Aggregator[Employee, Average, Double] {
      // A zero value for this aggregation. Should satisfy the property that any b + zero = b
      def zero: Average = Average(0L, 0L)
      // Combine two values to produce a new value. For performance, the function may modify `buffer`
      // and return it instead of constructing a new object
      def reduce(buffer: Average, employee: Employee): Average = {
        buffer.sum += employee.salary
        buffer.count += 1
        buffer
      }
      // Merge two intermediate values
      def merge(b1: Average, b2: Average): Average = {
        b1.sum += b2.sum
        b1.count += b2.count
        b1
      }
      // Transform the output of the reduction
      def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count
      // Specifies the Encoder for the intermediate value type
      def bufferEncoder: Encoder[Average] = Encoders.product
      // Specifies the Encoder for the final output value type
      def outputEncoder: Encoder[Double] = Encoders.scalaDouble
    }
    ```
    
[Spark SQL UDAF示例代码: SparkSQLUDAF.scala](../src/main/scala/org/spark/notes/SparkSQLUDAF.scala)
   
