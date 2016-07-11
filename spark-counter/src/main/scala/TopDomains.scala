import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark._

case class domain(name: String, count: BigInt)
case class Row(name:String, count:BigInt)


/**
  * Created by Kevin Duraj
  */
object TopDomains {

  val locale = new java.util.Locale("us", "US")
  val formatter = java.text.NumberFormat.getIntegerInstance(locale)
  
  def main(args: Array[String]) {
    val table_name = if(args(0).length == 0) "vdomain" else args(0)
    val size = if(args(1).length == 0) 10000 else args(1).toInt

    println(table_name + " " + size)
    get_largest_visited_domains(table_name, size)
//    large_domain(100)
  }

  def get_largest_visited_domains(table_name: String, size: Int): Unit = {

    val conf = new SparkConf(true).setAppName("Total")
    val sc   = new SparkContext(conf)
    val csc  = new CassandraSQLContext(sc)

    val df = csc.read.format("org.apache.spark.sql.cassandra")
              .options(Map( "keyspace" -> "cloud4", "table" -> table_name ))
              .load
    val df2 = df.select("domain", "total").filter("total > " + size).orderBy("total")
//    df2.filter("total > " + size)
    df2.collect().foreach { row => println(row.get(0)  + " " + row.get(1) ) }
//    df2.printSchema()


    // df2.filter("count > 1000").collect().foreach(println)
    // csc.setKeyspace("engine")
    // df2.registerTempTable("table1")
    // csc.sql("SELECT * FROM table1").show()

    //println("--------------------------------------------------------------")
    //df2.collect().foreach { row => println(row.get(0)  + " " + row.get(1) ) }
    //println("--------------------------------------------------------------")

  }
  /**
    * Largest domain names from engine.domain
    */
  def large_domain(size: Int): Unit = {

    val conf = new SparkConf(true).setAppName("LargeDomains")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)

    val domain_sql =
      """
        SELECT domain, total
         FROM test.vdomain
         ORDER BY total DESC LIMIT """ + size + """
        """.stripMargin

    val result_df = hc.sql(domain_sql)
    result_df.map(row => formatter.format(row(1)) + "\t" + row(0)).collect().foreach(println)

    //hc.sql("CREATE TABLE kduraj (domain TEXT, total INT, PRIMARY KEY(domain));")

  }

}
