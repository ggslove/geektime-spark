import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext


val seq: Seq[(String, Int)] = Seq(("Bob", 14), ("Alice", 18))
val rdd: RDD[(String, Int)] = sc.parallelize(seq)

import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}

//定义StructType
val schema: StructType = StructType(Array(
  StructField("name", StringType),
  StructField("age", IntegerType)
))


import org.apache.spark.sql.Row

// 将rdd转换为row 才可以转为 DataFrame
val rowRDD: RDD[Row] = rdd.map(fileds => Row(fileds._1, fileds._2))

import org.apache.spark.sql.DataFrame

val dataFrame1: DataFrame = spark.createDataFrame(rowRDD, schema)

import spark.implicits._ // spark.implicits 才可以用 seq.toDF

val dataFrame2: DataFrame = seq.toDF // 直接把seq 转为 DataFrame 用_1,_2表示field


/**
 * 读取 csv
 * |  参数     |  可选值       |    默认值    |       含义                |
 * |  header  |  true ,false |     false   | csv 第一行是否为DataSchema |
 * |  seq     |  任意字符     |    ,(逗号)   | 列分隔符                  |
 * |  escape  |  任意        |   \ 反斜杠   |  转义字符                 |
 * | nullValue| 任意         |   "" 空字符   |  文件空值                 |
 * | dateFormat| 日期格式字符串|  yyyy-MM-dd  |   日期格式               |
 * |mode      |  模式        | permissive   |  读取模式                |
 *
 * mode: permissive 容忍度高：将脏数据替换为null，正常加载数据并创建DataFrame
 * dropMalformed  有的商量：遇到脏数据则跳过加载
 * failFast       任性： 遇到脏数据立即报错
 */

val csvFilePath = "/Users/ggslove/CS/my_git_hub/Tutoring_geektime/Zero-based-spark/demo.csv"
val dfCSV: DataFrame = spark.read.format("csv").option("header", true).load(csvFilePath)
dfCSV.show
dfCSV.printSchema // 这样 两个字段类型都会变成string
//root
//|-- name: string (nullable = true)
//|-- age: string (nullable = true)

val schema2: StructType = StructType(Array(
  StructField("name", StringType),
  StructField("age", IntegerType)
))


val dfCSV2: DataFrame = spark.read.format("csv").schema(schema2).option("header", true).load(csvFilePath)

// 从 RDBMS创建DataFrame

val tbDf: DataFrame = spark.read.format("jdbc").option("driver", "com.mysql.jdbc.Driver")
  .option("url", "")
  .option("user", "")
  .option("password", "")
  .option("numPartitions", 20)
  .option("dbtable", "数据表名")
  .load()

val sqlQuery: String =
“select * from users where gender =
‘female
’”
spark.read.format("jdbc")
  .option("driver", "com.mysql.jdbc.Driver").
  option("url", "jdbc:mysql://hostname:port/mysql")
  .option("user", "用户名").option("password", "密码")
  .option("numPartitions", 20)
  .option("dbtable", sqlQuery).load()
// submit
// –driver-class-path mysql-connector-java-version.jar
// –jars mysql-connector-java-version.jar


