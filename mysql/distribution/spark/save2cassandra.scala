/*-------------------------------------------------------------*/
// dse spark -i:safe2cassandra.scala
/*-------------------------------------------------------------*/
import org.apache.spark.sql.functions._

csc.setKeyspace("cloud4")

val SQL = """SELECT url FROM cloud4.link3 WHERE url LIKE '%cancer%'"""

val df1 = csc.sql(SQL).toDF()
//val rdd1 = df1.rdd
//rdd1.saveToCassandra("cloud4", "link_clean", SomeColumns("url"))
//rdd1.count()
//rdd1.show()

df1.show(12, false)


sys.exit
/*-------------------------------------------------------------*/
