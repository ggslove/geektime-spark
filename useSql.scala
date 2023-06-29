import org.apache.spark.sql.DataFrame

import spark.implicits._

val seq: Seq[(String, Int)] = Seq(("Alice", 18), ("Bob", 14))
val df = seq.toDF("name", "age")

df.createTempView("t1")
val query: String = "select * from t1"

val result: DataFrame = spark.sql(query)

result.show
result.orderBy()



// dropDuplicates 挺奇葩的
employeesDF.show

/** 结果打印
 * +---+-------+---+------+
 * | id| name|age|gender|
 * +---+-------+---+------+
 * | 1| John| 26| Male|
 * | 2| Lily| 28|Female|
 * | 3|Raymond| 30| Male|
 * +---+-------+---+------+
 */

employeesDF.dropDuplicates("gender").show

/** 结果打印
 * +---+----+---+------+
 * | id|name|age|gender|
 * +---+----+---+------+
 * | 2|Lily| 28|Female|
 * | 1|John| 26| Male|
 * +---+----+---+------+
 */

// 可以加sql表达式

employeesDF.selectExpr("id", "name", "concat(id, '_', name) as id_name").show

/** 结果打印
 * +---+-------+---------+
 * | id| name| id_name|
 * +---+-------+---------+
 * | 1| John| 1_John|
 * | 2| Lily| 2_Lily|
 * | 3|Raymond|3_Raymond|
 * +---+-------+---------+
 */

// withColumn生成新列

employeesDF.withColumn("crypto", hash($"age")).show

/** 结果打印
 * +---+-------+---+------+-----------+
 * | id| name|age|gender| crypto|
 * +---+-------+---+------+-----------+
 * | 1| John| 26| Male|-1223696181|
 * | 2| Lily| 28|Female|-1721654386|
 * | 3|Raymond| 30| Male| 1796998381|
 * +---+-------+---+------+-----------+
 */

val seq = Seq((1, "John", 26, "Male", Seq("1", "2")),
  (2, "Lily", 28, "Female", Seq("3", "4")),
  (3, "Raymond", 30, "Male", Seq("5", "6"))
)

val employeesDF: DataFrame = seq.toDF("id", "name", "age", "gender", "interests")
employeesDF.show

/** 结果打印
 * +---+-------+---+------+-------------------+
 * | id| name|age|gender| interests|
 * +---+-------+---+------+-------------------+
 * | 1| John| 26| Male| [Sports, News]|
 * | 2| Lily| 28|Female|[Shopping, Reading]|
 * | 3|Raymond| 30| Male| [Sports, Reading]|
 * +---+-------+---+------+-------------------+
 */

employeesDF.withColumn("interest", explode($"interests")).show // 原来1条数据根据数组展开多少条

/** 结果打印
 * +---+-------+---+------+-------------------+--------+
 * | id| name|age|gender| interests|interest|
 * +---+-------+---+------+-------------------+--------+
 * | 1| John| 26| Male| [Sports, News]| Sports|
 * | 1| John| 26| Male| [Sports, News]| News|
 * | 2| Lily| 28|Female|[Shopping, Reading]|Shopping|
 * | 2| Lily| 28|Female|[Shopping, Reading]| Reading|
 * | 3|Raymond| 30| Male| [Sports, Reading]| Sports|
 * | 3|Raymond| 30| Male| [Sports, Reading]| Reading|
 * +---+-------+---+------+-------------------+--------+
 */


// --------------->  joiin


val seq = Seq((1, "Mike", 28, "Male"), (2, "Lily", 30, "Female"), (3, "Raymond", 26, "Male"))
val employees: DataFrame = seq.toDF("id", "name", "age", "gender")
// 创建薪水表
val seq2 = Seq((1, 26000), (2, 30000), (4, 25000), (3, 20000))
val salaries: DataFrame = seq2.toDF("id", "salary")
employees.show


val jointDF: DataFrame = salaries.join(employees, Seq("id"), "inner")

jointDF.show

/** 结果打印
+---+------+-------+---+------+
| id|salary| name|age|gender|
+---+------+-------+---+------+
| 1| 26000| Mike| 28| Male|
| 2| 30000| Lily| 30|Female|
| 3| 20000|Raymond| 26| Male|
+---+------+-------+---+------+
 */


val aggResult = jointDF.groupBy("gender").agg(sum("salary").as("sum_salary"), avg("salary").as("avg_salary"))

aggResult.show

/** 数据打印
+------+----------+----------+
|gender|sum_salary|avg_salary|
+------+----------+----------+
|Female| 30000| 30000.0|
| Male| 46000| 23000.0|
+------+----------+----------+
 */

aggResult.sort(desc("sum_salary"), asc("gender")).show