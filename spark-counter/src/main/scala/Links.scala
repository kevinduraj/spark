import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.types.{IntType, TextType}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by kevin duraj 
  */
case class LinkCase(domain: String)

object Links {

  def main(args: Array[String]) {

    cassandra_context_group_by_display(args)
    //spark_context_group_by(args)
    //cassandra_context_group_by_save(args)
  }

  /**
    * Using Spark Context
    */
  def spark_context_group_by(args: Array[String]): Unit = {

    val table_name = if (args.length == 0) "result99" else args(0)
    val conf = new SparkConf(true).setAppName("Links")
    val sc = new SparkContext(conf)

    val health = sc.cassandraTable("engine", "link").select("domain").as(LinkCase)
    val health_map = health.map(s => (s.domain, 1))
    val rdd1 = health_map.reduceByKey(_ + _)
    // result.collect().foreach(println)
    rdd1.saveAsCassandraTable("engine", table_name, SomeColumns("_1", "_2"))

  }

  /**
    * Using Cassandra Context
    *
    * @param args limit argument
    */
  def cassandra_context_group_by_display(args: Array[String]): Unit = {

    val conf = new SparkConf(true).setAppName("Links")
    val sc = new SparkContext(conf)
    val limit = if (args.length == 0) 50 else args(0)
 
    val csc = new CassandraSQLContext(sc)
    csc.setKeyspace("engine")

    val SQL = "SELECT distinct domain, count(domain) total " +
              " FROM link " +
              " GROUP BY domain " +
              " ORDER BY total DESC LIMIT " + limit

    // display results
    csc.sql(SQL).collect().foreach { row => println(row.get(0) + " " + row.get(1)) }
  }

  def cassandra_context_group_by_save(args: Array[String]): Unit = {

    val table_name = if (args.length == 0) "kduraj" else args(0)
    val conf = new SparkConf(true).setAppName("Links")
    val sc = new SparkContext(conf)
    val csc = new CassandraSQLContext(sc)
    csc.setKeyspace("engine")

    val SQL = "SELECT distinct domain, count(domain) total " +
              " FROM link" +
              " GROUP BY domain " +
              " ORDER BY total DESC LIMIT 100"

    val df1 = csc.sql(SQL).toDF()
    //create_table(table_name)

    df1.write.format("org.apache.spark.sql.cassandra")
      .options(Map( "keyspace" -> "engine", "table" -> table_name ))
      .mode(SaveMode.Overwrite)
      .save()
  }

  def create_table(table_name: String): Unit = {
    
    val conf  = new SparkConf(true).setAppName("Links")
    val sc    = new SparkContext(conf)

    val pkey  = ColumnDef("domain", PartitionKeyColumn, TextType)
    val group = ColumnDef("total", ClusteringColumn(0), IntType)
    val value = ColumnDef("value", RegularColumn, IntType)

    val table = TableDef("engine", table_name, Seq(pkey), Seq(group), Seq(value))
    //val rows = Seq(("key1", 1, "value1"), ("key2", 2, "value2"), ("key3", 3, "value3"))
    val rows = Seq(("null", 0, 0))

    sc.parallelize(rows).saveAsCassandraTableEx(table, SomeColumns("key", "group", "value"))

  }

}
