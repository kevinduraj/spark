/*
  dse spark -i:joining-tables.scala
*/
import org.apache.spark.sql.functions._

case class LeftTable  (part: String, author: String) 
case class RightTable (part: String, clust: String, book: String) 
case class BothTables (part: String, clust: String, author: String, book: String) 

val left  = sc.cassandraTable[LeftTable]("test_join", "left_table")
val right = sc.cassandraTable[RightTable]("test_join", "right_table")

val leftByUserId  = left.keyBy (col => col.part);
val rightByUserId = right.keyBy(col => col.part);

//Join the tables by the user_part
val joinedTables = leftByUserId.join(rightByUserId).cache

//Create RDD with a the new object type which maps to our new table
val bothRDD = joinedTables.map({ case (key, (left, right)) => new BothTables(left.part, right.clust, left.author, right.book)})
bothRDD.saveToCassandra("test_join", "big_table")

/* Top ten results reverse sorted result 
val top10 = bothRDD.collect.toList.sortBy(_.author).reverse.take(10)
top10.foreach(println)
*/

/* Take the entire result set
val result = bothRDD.collect()
val NewTableRDD = sc.parallelize(result);
NewTableRDD.saveToCassandra("test_join", "big_table")
*/

sys.exit()
