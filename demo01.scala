import org.apache.spark.rdd.RDD

val rootPath="."
val file:String=s"${rootPath}/wikiOfSpark.txt"
val lineRDD:RDD[String]=spark.sparkContext.textFile(file)
val wordRDD:RDD[String]=lineRDD.flatMap(line=> line.split(" "))
val cleanWordRDD:RDD[String]=wordRDD.filter(word=> !"".equals(word))
val kvRDD:RDD[(String,Int)]=cleanWordRDD.map(word=> (word,1))
val wordCounts:RDD[(String,Int)]=kvRDD.reduceByKey((x,y)=> x+y)
wordCounts.map{case (k,v)=>(v,k)}.sortByKey(false).take(5)
