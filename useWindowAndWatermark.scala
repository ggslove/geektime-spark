import org.apache.spark.sql.DataFrame

val host: String = "127.0.0.1"
val port: String = "9999"
val df: DataFrame = spark.readStream.format("socket").option("host", host).option("port", port).load


df.withColumn("inputs", split($"value", ",")).
  withColumn("eventTime", element_at(col("inputs"), 1).cast("timestamp")).
  withColumn("words", split(element_at(col("inputs"), 2), " ")).
  withColumn("word", explode($"words")).
  groupBy(window(col("eventTime"), "5 minute"), col("word")).
  count().
  writeStream.
  format("console").
  option("truncate", false).
  outputMode("complete").
  start().
  awaitTermination()


/**
 * // 按照Tumbling Window与单词做分组
 * .groupBy(window(col("eventTime"), "5 minute"), col("word"))
 *
 * 2021-10-01 09:30:00,Apache Spark
 * 2021-10-01 09:34:00,Spark Logo
 * 2021-10-01 09:36:00,Structured Streaming
 * 2021-10-01 09:39:00,Structured Streaming
 * 2021-10-01 09:41:00,A
 */


df.withColumn("inputs", split($"value", ",")).
  withColumn("eventTime", element_at(col("inputs"), 1).cast("timestamp")).
  withColumn("words", split(element_at(col("inputs"), 2), " ")).
  withColumn("word", explode($"words")).
  groupBy(window(col("eventTime"), "5 minute", "3 minute"), col("word")). // 5minute 指的是 每个窗口5分钟，3minute指的是移动3分钟
  count().
  writeStream.
  format("console").
  option("truncate", false).
  outputMode("complete").
  start().
  awaitTermination()

// 2021-10-01 09:15:00,Apache Spark
// watermark 如何处理
/**
  2021-10-01 09:30:00,Apache Spark
  2021-10-01 09:34:00,Spark Logo
  2021-10-01 09:36:00,Structured Streaming
  2021-10-01 09:39:00,Structured Streaming
  2021-10-01 09:41:00,AMP Lab
  2021-10-01 09:44:00,SPARK SQL
  2021-10-01 09:29:00,Test Test
  2021-10-01 09:33:00,Spark is Cool


   2021-10-01 09:19:00,Test Test
 */

 df.withColumn("inputs", split($"value", ",")).
    withColumn("eventTime", element_at(col("inputs"),1).cast("timestamp")).// 提取事件时间
    withColumn("words", split(element_at(col("inputs"),2), " ")). // 提取单词序列
    withColumn("word", explode($"words")). // 拆分单词
    withWatermark("eventTime", "10 minute"). // 启用Watermark机制，指定容忍度T为10分钟
    groupBy(window(col("eventTime"), "5 minute"), col("word")). // 按照Tumbling Window与单词做分组
    count().  // 统计计数
  writeStream.
  format("console").
  option("truncate", false).
  outputMode("Update"). // complete 时 watermark 没有用
  start().
  awaitTermination()
