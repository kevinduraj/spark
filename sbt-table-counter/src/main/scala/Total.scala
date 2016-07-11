/**
  * Created by kevin duraj 
  * Count Cassandra rows
  */

import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.SortedMap

case class TotalCase(category_name: String)

object Total {

  val locale = new java.util.Locale("us", "US")
  val formatter = java.text.NumberFormat.getIntegerInstance(locale)

  def main(args: Array[String]) {

    val conf = new SparkConf(true).setAppName("Total")
    val sc = new SparkContext(conf)

    val link1 = sc.cassandraTable("cloud2", "vdomain").select("url").as(TotalCase)
    val link2 = sc.cassandraTable("cloud2", "visit").select("url").as(TotalCase)
    val link3 = sc.cassandraTable("cloud2", "ldomain").select("url").as(TotalCase)
    val link4 = sc.cassandraTable("cloud2", "link").select("url").as(TotalCase)

    var map = SortedMap[Int, String]()

    map += (link1.cassandraCount().toInt -> "vDomain : ")
    map += (link2.cassandraCount().toInt -> "Visit   : ")
    map += (link3.cassandraCount().toInt -> "lDomain : ")
    map += (link4.cassandraCount().toInt -> "Link    : ")

    println("-----------------------------------------")
    map.foreach { row =>
      println(row._2 + " " + formatter.format(row._1))
    }
    println("-----------------------------------------")
  }

}
