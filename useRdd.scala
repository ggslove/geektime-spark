import org.apache.spark.rdd.RDD

val rootPath = "."
val file: String = s"${rootPath}/wikiOfSpark.txt"
val lineRDD: RDD[String] = spark.sparkContext.textFile(file)
val wordRDD: RDD[String] = lineRDD.flatMap(line => line.split(" "))
val cleanWordRDD: RDD[String] = wordRDD.filter(word => !"".equals(word))
val kvRDD: RDD[(String, Int)] = cleanWordRDD.map(word => (word, 1))
val wordCounts: RDD[(String, Int)] = kvRDD.reduceByKey((x, y) => x + y)
wordCounts.map { case (k, v) => (v, k) }.sortByKey(false).take(5)

val wordPairRDD: RDD[String] = lineRDD.flatMap(line => {
  val words: Array[String] = line.split(" ")
  for (i <- 0 until words.length - 1) yield words(i) + "-" + words(i + 1)

})

val kvRDD2: RDD[(String, Int)] = wordPairRDD.map(word => (word, 1))
val wordCounts2: RDD[(String, Int)] = kvRDD2.reduceByKey((x, y) => x + y)
wordCounts2.map { case (k, v) => (v, k) }.sortByKey(false).take(5)


val words: Array[String] = Array("Spark", "is", "cool")
val rdd: RDD[String] = sc.para llelize (words) // 内部创建数据

def f(word: String): (String, Int) = {
  return (word, 1)
}

val kvRDD: RDD[(String, Int)] = rdd.map(f)

def f2(word: String): (String, Int) = {
  val md5 = MessageDigest.getInstance("MD5")
  val hash = md5.digest(word.getBytes).mkString
  (hash, 1)
}

val kvRDD2: RDD[(String, Int)] = rdd.map(f2)
// 方法参数需要指定对象
def f3(it: Iterator[String]) = {
  val md5 = MessageDigest.getInstance("MD5")
  val newIter = it.map(word => md5.digest(word.getBytes).mkString)
  newIter
}

val rdd3 = rdd.mapPartitions(f3)

// 这样就可以用，不知道为啥上面不能用
// 在这里 使用mapPartitions 让md5 在每个mapParitition中可以共用
val rdd3 = rdd.mapPartitions(iter => {
  val md5 = MessageDigest.getInstance("MD5")
  iter.map(word => md5.digest(word.getBytes).mkString)
})
rdd3.count()

// flatMap 从元素到集合， 再从集合到元素

val lines: Array[String] = Array("Apache Spark",
  "From Wikipedia, the free encyclopedia",
  "Jump to navigationJump to search")
val lineRDD: RDD[String] = sc.parallelize(lines) // 内部创建数据


val wordPairRDD: RDD[String] = lineRDD.flatMap(line => {
  val words: Array[String] = line.split(" ")
  for (i <- 0 until words.length - 1) yield words(i) + "-" + words(i + 1) // flatMap 返回的是一个 数组
})
//js 中 [1,2,3].flatMap(x=>[x,x*2])  也会变成  1,2,2,4,3,6


//count =9
//Apache-Spark
//From-Wikipedia,
//Wikipedia,-the
//the-free
//free-encyclopedia
//Jump-to
//to-navigationJump
//navigationJump-to
//to-search
val wordPairRDD2: RDD[String] = lineRDD.map(line => {
  //  val words: Array[String] = line.split(" ")
  //  for (i <- 0 until words.length - 1) yield words(i) + "-" + words(i + 1)
  line + "<ahahaha>"
})

