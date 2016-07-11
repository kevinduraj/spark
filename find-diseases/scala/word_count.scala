/* load flat file with text */
val input = sc.textFile("file:///home/temp/test.txt")

// split flat text into words
val words = input.flatMap(x => x.split(" "))

// group by words and count
val result = words.map(x => (x, 1)).reduceByKey((x, y) => x + y)

// print sample 100 results
result.take(100).foreach(println)

// print all the results
result.collect().foreach(println)
