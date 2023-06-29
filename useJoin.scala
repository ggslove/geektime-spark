
import spark.implicits._
import org.apache.spark.sql.DataFrame

// 创建员工信息表
val seq = Seq((1, "Mike", 28, "Male"), (2, "Lily", 30, "Female"), (3, "Raymond", 26, "Male"), (5, "Dave", 36, "Male"))
val employees: DataFrame = seq.toDF("id", "name", "age", "gender")

// 创建薪资表
val seq2 = Seq((1, 26000), (2, 30000), (4, 25000), (3, 20000))
val salaries: DataFrame = seq2.toDF("id", "salary")






// 左表
salaries.show

/** 结果打印
 * +---+------+
 * | id|salary|
 * +---+------+
 * | 1| 26000|
 * | 2| 30000|
 * | 4| 25000|
 * | 3| 20000|
 * +---+------+
 */

// 右表
employees.show

/** 结果打印
 * +---+-------+---+------+
 * | id| name|age|gender|
 * +---+-------+---+------+
 * | 1| Mike| 28| Male|
 * | 2| Lily| 30|Female|
 * | 3|Raymond| 26| Male|
 * | 5| Dave| 36| Male|
 * +---+-------+---+------+
 */

// 内关联
val joinDF: DataFrame = salaries.join(employees, salaries("id") === employees("id"), "inner") // left,right,full
joinDF.show

/** 结果打印
 * +---+------+---+-------+---+------+
 * | id|salary| id| name|age|gender|
 * +---+------+---+-------+---+------+
 * | 1| 26000| 1| Mike| 28| Male|
 * | 2| 30000| 2| Lily| 30|Female|
 * | 3| 20000| 3|Raymond| 26| Male|
 * +---+------+---+-------+---+------+
 */


// 左半关联
val jointDF: DataFrame = salaries.join(employees, salaries("id") === employees("id"), "leftsemi")

jointDF.show

/** 结果打印
 * +---+------+
 * | id|salary|
 * +---+------+
 * | 1| 26000|
 * | 2| 30000|
 * | 3| 20000|
 * +---+------+
 */

// leftanti 与 leftsemi 相反 左逆关联   查询 没有join的数据
val jointDF: DataFrame = salaries.join(employees, salaries("id") === employees("id"), "leftanti")
// spark.sql("select * from employees t1 left semi join  salaries t2 on  (t1.id=t2.id or t1.id=5) ").show
//  spark.sql("select * from employees t1 left anti join  salaries t2 on  (t1.id=t2.id or t1.id=5) ").show
jointDF.show

/** 结果打印
 * +---+------+
 * | id|salary|
 * +---+------+
 * | 4| 25000|
 * +---+------+
 */


// 右关联
val jointDF: DataFrame = salaries.join(employees, salaries("id") === employees("id"), "rightsemi")

jointDF.show

//select * from employees t1  left semi on salaries t2 where
// left anti


//  验证 left anti


val seq = Seq(("1", "a"), ("2", "b"), ("3", "c"))
val t1: DataFrame = seq.toDF("id", "name")

val seq2 = Seq(("1", "a"), ("2", "c"), ("4", "d"))
val t2: DataFrame = seq2.toDF("id", "name")

t1.createTempView("test1")
t2.createTempView("test2")




//左半关联
spark.sql("select  * from test1 t1 left semi join test2 t2 on (t1.id=t2.id)").show;
/**
 * +---+----+
 * | id|name|
 * +---+----+
 * |  1|   a|
 * |  2|   b|
 * +---+----+
 */
spark.sql("select  * from test1 t1 left anti join test2 t2 on (t1.id=t2.id)").show;
/**
 * +---+----+
 * | id|name|
 * +---+----+
 * |  3|   c|
 * +---+----+
 */

spark.sql("select  * from test1 t1 left semi join test2 t2 on (t1.id=t2.id and t1.name=t2.name)").show;
/**
 * +---+----+
 * | id|name|
 * +---+----+
 * |  1|   a|
 * +---+----+
 */


spark.sql("select  * from test1 t1 left anti join test2 t2 on (t1.id=t2.id and t1.name=t2.name)").show;
/**
 * +---+----+
 * | id|name|
 * +---+----+
 * |  2|   b|
 * |  3|   c|
 * +---+----+
 */





