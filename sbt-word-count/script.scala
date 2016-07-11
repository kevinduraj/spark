val inputFile = sc.textFile("/home/temp/may-link2.dat")

val counts = inputFile.flatMap(line => line.split(" ").map(word => (word, 1)).reduceByKey(_ + _);

counts.toDebugString

counts.cache()

counts.repartition(5)

counts.saveAsTextFile("/home/temp/output")

counts.unpresists()


