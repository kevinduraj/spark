val myTable = sc.cassandraTable[(String, Int)]("domain", "table1")

myTable.count()

myTable.take(10).foreach(println)

val count2 = myTable.collect().toList.sortBy{ - _._2 }

case class Row(url:String, count:Int)

val count3 = count2.map( r=> Row( r._1, r._2) ).toDF()

val count4 = count3.filter(_.count > 10000)

count4.take(100).foreach(println)

