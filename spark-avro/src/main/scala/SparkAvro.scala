import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import com.databricks.spark.avro._

object SparkAvro {

  /*----------------------------------------------------------------------------*/
  //                           Main 
  /*----------------------------------------------------------------------------*/
  def main(args: Array[String]) {

    //-- Enable WARN --//
    //Logger.getLogger("org").setLevel(Level.WARN)
    //Logger.getLogger("akka").setLevel(Level.WARN)

    //-- Retrieve command line parameters --//
    // val threshold = args(1).toInt
    // val tokenized = sc.textFile(args(0)).flatMap(_.split(" "))

    val inputFile = args(0); println("Input File: " + inputFile)

    read_avro_format(inputFile)

  }

  /*----------------------------------------------------------------------------*/
  //                        Read Avro Format 
  // input columns: [title, air_date, doctor]
  /*----------------------------------------------------------------------------*/
  def read_avro_format(inputFile: String) {

    println("Reading Avro File")

    val sc = new SparkContext(new SparkConf().setAppName("SparkAvro"))
    val sqlContext = new SQLContext(sc)
    
    val df1 = sqlContext.read.format("com.databricks.spark.avro").load("src/test/resources/episodes.avro")
    df1.printSchema()
    df1.show()
    println("------- Filter By Doctor > 5 -----")
    df1.filter(df1("doctor") > 5).show()

    println("--------- GroupBy Doctor ---------")
    df1.groupBy("doctor").count().show() 

    println("\n-------------- doctor > 10 --------------------")
    df1.filter("doctor > 10").write.format("com.databricks.spark.avro").save("/tmp/spark/avro")

    val df2 = sqlContext.read.format("com.databricks.spark.avro").load("/tmp/spark/avro")
    df2.select("title", "doctor").show()
    df2.foreach(println)
    
  }

}


