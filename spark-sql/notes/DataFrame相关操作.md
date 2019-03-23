Spark SQL DataFrame相关操作
------
* [Contents](#Contents)
	* [打印DataFrame里面的模式](#打印DataFrame里面的模式)
	* [查询DataFrame里面的数据](#查询DataFrame里面的数据)
	* [对DataFrame里面的数据进行排序](#对DataFrame里面的数据进行排序)
	* [对DataFrame列进行重命名](#对DataFrame列进行重命名)
	* [对DataFrame列进行类型转换](#对DataFrame列进行类型转换)
	* [对DataFrame删除增加列](#对DataFrame删除增加列)
	* [对DataFrame列进行正则处理](#对DataFrame列进行正则处理)
	* [对DataFrame列进行聚合操作](#对DataFrame列进行聚合操作)
	* [对DataFrame的缺失值(Null)值处理](#对DataFrame的缺失值Null值处理)
	* [对DataFrame进行join操作](#对DataFrame进行join操作)
	* [对DataFrame进行union操作](#对DataFrame进行union操作)
	* [DataFrame窗口函数使用](#DataFrame窗口函数使用)
	* [对DataFrame数据转JSON](#对DataFrame数据转JSON)
	
Spark SQL中的DataFrame类似于一张关系型表。在关系型数据库中可以对单表或多表进行查询操作，
在DataFrame中都通过调用其API层面来实现。可以參考，Scala提供的[DataFrame API](http://spark.apache.org/docs/2.2.0/api/scala/index.html#org.apache.spark.sql.Dataset)。
## 打印DataFrame里面的模式
可以通过printSchema函数来查看。它会打印出列的名称和类型，是否能为null
```scala
    // 打印schema
    df.printSchema()
    // 查看列的类型
    df.dtypes
    // 查看列名
    df.columns
    // 查看行数
    df.count
```
## 查询DataFrame里面的数据
可以使用show方法进行简单查看：
可以指定采样的行数def show(numRows: Int)。不指定指定任何参数，show函数默认会加载出20行的数据def show()
可以指定一个boolean值，这个值说明是否需要对超过20个字符的列进行截取def show(truncate: Boolean)；
还可以使用def select(cols: Column*)进行列选择。还可以使用def filter(conditionExpr: String)进行过滤。
```scala
    df.select("id", "name", "age").show(3)
    df.select(df("id"), df("name"), df("age")).show(true)
    df.select($"id", $"name", $"age").show(3,true)
```
## 对DataFrame里面的数据进行排序
可以使用sort函数对DataFrame中指定的列进行排序。
```scala
    // 默认升序
    df.sort("age").show()
    // 降序排序
    df.sort($"age".desc).show()
    //多字段排序 混合排序
    df.sort($"id".desc, $"age".asc).show()
```
## 对DataFrame列进行重命名
对DataFrame中默认的列名不感兴趣，我们可以在select的时候利用as对其进行重命名，
也可以使用def withColumnRenamed(existingName: String, newName: String)方法进行重命名。
```scala
    // withColumnRenamed方法 ==> 这种方式会返回新的DF包含所有列  推荐
    df.withColumnRenamed("id" , "uid").show()
    // alias 方法 ==> 这种方式会返回新的DF只有uid一列
    df.select(df("id").alias("uid")).show()
    df.select(df("id") as "uid", df("id").as("uid2")).show()
```
## 对DataFrame列进行类型转换
如果对拿到的DataFrame列的类型不满意是，可以通过cast进行类型转换。
```scala
    df.selectExpr("cast(id as int) as id", "name", "cast(age as int) age")
    df.select(df("id").cast("int"), df("name"), df("age").cast("int"))
```

## 对DataFrame删除增加列
在需要时，可以对DataFrame的列进行删除，新成一个新的DataFrame，也可以主动加入新列。
```scala
    // 删除列
    df.drop("id").show()
    // 增加列
    df.withColumn("newAge", df("age") + 10).show()
    // 增加常量列
    df.selectExpr("*", "1 one").show()
    // 通过lit方法增加常量列
    df.select(col("id"), lit(1).as("one")).show()
```

## 对DataFrame列进行正则处理
可以使用def regexp_extract(e: Column, exp: String, groupIdx: Int)采用Java正则对列进行处理，
并返回匹配的指定分组值。也可以使用def regexp_replace(e: Column, pattern: String, replacement: String)
进行正则替换。
```scala
    df.select($"id",
      regexp_extract($"name", "([A-Za-z]+)(_[0-9]?)?", 1).as("name")
      , $"age").show()
    df.select($"id",
      regexp_replace($"name", "([A-Za-z]+)(_[0-9]+)?", "aaa").as("name")
      , $"age").show()
```
## 对DataFrame列进行聚合操作
聚合操作调用的是agg方法，该方法有多种调用方式。一般与groupBy方法配合使用。
```scala
    df.agg(max("age"), min("age")).show()
    df.agg(Map("age"->"max", "age"->"min")).show()
    df.agg(("age", "max"), ("age", "min")).show()
    // min max count 等函数必须放在agg里
    df.groupBy("age").agg(min("id")).show()
```
## 对DataFrame的缺失值Null值处理
可以使用na.fill(map)，实现对NULL值的填充。
```scala
    // null 统一替换成 ""
    df.na.fill("").show()
    // 根据不同列进行null值替换
    df.na.fill(Map("name"->"", "age"-> -1)).show()
```
## 对DataFrame进行join操作
通过join函数实现两个DataFrame的连接操作，并要指定连接字段。默认的连接方式是inner，
当然也可以使用其他的方式，通过第三个参数来指定，可以指定的类型有`inner`, `cross`,
`outer`, `full`, `full_outer`, `left`, `left_outer`,`right`, `right_outer`,
`left_semi`, `left_anti`类型。
```scala
    // 默认inner
    df1.join(df2,"id").show()
    // `inner`, `cross`, `outer`, `full`, `full_outer`, `left`, `left_outer`,`right`, `right_outer`, `left_semi`, `left_anti`
    df1.join(df2, Seq("id"), "right").show()
    // 必须使用===代替等于
    df1.join(df2, df1("id") === df2("id") ).show()
```
## 对DataFrame进行union操作
可以对结构相同的两个DataFrame进行union操作。注意Spark SQL中union和hql的union不一样，
只是合并不会去重相当于SQL中的UNION ALL操作。其中unionAll方法底层也是调用union，已经过时。
```scala
df1.union(df2).show()
``` 
## DataFrame窗口函数使用
可以对DataFrame使用窗口函数row_number()、rank()等。
```scala
    df.select($"id", $"name", $"age",
      row_number().over(Window.partitionBy("age").orderBy($"id".asc)).as("rank")
    )
```
## 对DataFrame数据转JSON
可以调用df.toJSON将Dataset[Row]转成Dataset[String] json字符串。

[**DataFrame操作示例代码: DataFrameOperate.scala**](../src/main/scala/org/spark/notes/DataFrameOperate.scala)
  


