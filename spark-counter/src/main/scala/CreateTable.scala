import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.types._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create Casandra Table using Spark
  */
object CreateTable {

  val conf = new SparkConf(true).setAppName("CreateTable")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {
    
    val table_name = if(args(0).length == 0) "kduraj" else args(0)

    val pkey  = ColumnDef("key", PartitionKeyColumn, TextType)
    val group = ColumnDef("group", ClusteringColumn(0), IntType)
    val value = ColumnDef("value", RegularColumn, TextType)

    val table = TableDef("engine", table_name, Seq(pkey), Seq(group), Seq(value))
    //val rows = Seq(("key1", 1, "value1"), ("key2", 2, "value2"), ("key3", 3, "value3"))
    val rows = Seq(("null", 0, "null"))

    sc.parallelize(rows).saveAsCassandraTableEx(table, SomeColumns("key", "group", "value"))

  }

}
