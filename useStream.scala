import org.apache.spark.sql.DataFrame

val host: String = "127.0.0.1"
val port: String = "9999"

val df: DataFrame = spark.readStream.format("socket").option("host", host).option("port", port).load
// nc -lk 9999
// 开启一个端口 然后等待输入，
// spark 开启一个监听，类似tomcat
df.withColumn("words", split($"value", " ")).
  withColumn("word", explode($"words")).
  groupBy("word").
  count().
  writeStream.
  trigger(processTime = "5 seconds").
  format("console").
  // 指定Sink为终端（Console）
  option("truncate", false).
  // 指定输出模式
  outputMode("complete").
  // Complete mode：输出到目前为止处理过的全部内容
  // Append mode：仅输出最近一次作业的计算结果
  // Update mode：仅输出内容有更新的计算结果
  start().
  // 启动流处理应用
  awaitTermination()
// 等待中断指令


// 计算模型
// Batch mode   trigger(processingTime("5 seconds"))
// Continuous mode trigger(continuous=" 1 second")
// 容错机制
/**
 * At most once：最多交付一次，数据存在丢失的风险；
 * At least once：最少交付一次，数据存在重复的可能；
 * Exactly once：交付且仅交付一次，数据不重不漏。
 * 利用 Checkpoint 机制来实现容错
 *
 */

