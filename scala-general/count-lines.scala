scala> val inputFile = sc.textFile("/home/temp/may-link2.dat")
inputFile: org.apache.spark.rdd.RDD[String] = /home/temp/may-link2.dat MapPartitionsRDD[1] at textFile at <console>:27

scala> val df = inputFile.toDF("url")
df: org.apache.spark.sql.DataFrame = [url: string]

scala> val health = df.filter(col("url").like("%health%"))
health: org.apache.spark.sql.DataFrame = [url: string]


val inputFile = sc.textFile("hdfs://hadoop-master:9000/user/hadoop/file.dat")
val df = inputFile.toDF("url")
val health = df.filter(col("url").like("%health%"))


