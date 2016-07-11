/*
  dse spark -i:joining-csc.scala
*/
csc.setKeyspace("test_join")

val result = csc.sql("SELECT t1.part, t1.author, t2.book from left_table t1 JOIN right_table t2 ON t1.part = t2.part")

result.take(20).foreach(println)

