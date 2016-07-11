/**
  * Created by kevin duraj 
  * Count Cassandra rows
  */

import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.SortedMap

case class VisitCase(category_name: String)

object Visit {

  val locale = new java.util.Locale("us", "US")
  val formatter = java.text.NumberFormat.getIntegerInstance(locale)

  def main(args: Array[String]) {

    val conf = new SparkConf(true).setAppName("Visited")
    val sc = new SparkContext(conf)

    val link1 = sc.cassandraTable("cloud2", "visit").select("url").as(VisitCase)
    val link2 = sc.cassandraTable("cloud2", "visit1").select("url").as(VisitCase)

    var map = SortedMap[Int, String]()

    map += (link1.cassandraCount().toInt -> "cloud2.visit  : ")
    map += (link2.cassandraCount().toInt -> "cloud2.visit1 : ")

    println("-----------------------------------------")
    map.foreach { row =>
      println(row._2 + " " + formatter.format(row._1))
    }
    println("-----------------------------------------")
  }

}
