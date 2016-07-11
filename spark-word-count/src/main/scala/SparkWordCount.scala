import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}

object SparkWordCount {

  val threshold = 3

  /*----------------------------------------------------------------------------*/
  //                           Main 
  /*----------------------------------------------------------------------------*/
  def main(args: Array[String]) {

    //-- Enable WARN --//
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    //-- Retrieve command line parameters --//
    // val threshold = args(1).toInt
    // val tokenized = sc.textFile(args(0)).flatMap(_.split(" "))

    val inputFile = args(0)
    val outputDir = args(1)
    println("Input File: " + inputFile)
    println("Output Dir: " + outputDir)

    //char_count()
    //word_count()
    //length_count()
    distinct_count(inputFile, outputDir)

  }

  /*----------------------------------------------------------------------------*/
  //                          Distinct
  /*----------------------------------------------------------------------------*/
  def distinct_count(inputFile: String, outputDir: String) {

    val sc = new SparkContext(new SparkConf().setAppName("Word Count"))

    val textFile = sc.textFile(inputFile)
    val words    = textFile.flatMap(line => line.split(" "))
    val distinct = words.distinct
                  
    println("=================================================")
    distinct.foreach(println)
    println("=================================================")
   
    distinct.saveAsTextFile(outputDir + "/distinct")
    //distinct.saveAsSequenceFile("out-sequence")
  }


  /*----------------------------------------------------------------------------*/
  //                          Word Count
  /*----------------------------------------------------------------------------*/
  def word_count(inputFile: String, outputDir: String) {

    val sc = new SparkContext(new SparkConf().setAppName("Word Count"))

    val textFile = sc.textFile(inputFile)
    val counts   = textFile.flatMap(line => line.split(" "))
    val words    = counts.map(word => (word, 1)).reduceByKey(_ + _)
    val filter   = words.filter(_._2 >= threshold)
                  
    println("=================================================")
    filter.foreach(println)
    println("=================================================")

  }

  /*----------------------------------------------------------------------------*/
  //                         Lenght Count
  /*----------------------------------------------------------------------------*/
  def length_count(inputFile: String, outputDir: String) {

    val sc = new SparkContext(new SparkConf().setAppName("Word Count"))
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val textFile = sc.textFile(inputFile)
    val words    = textFile.flatMap(line => line.split(" "))
    val lenght   = words.filter { l => l.length > 8 }
    val reduced  = lenght.map(word => (word, 1)).reduceByKey(_ + _)
                  
    println("=================================================")
    reduced.foreach(println)
    println("=================================================")

  }

  /*----------------------------------------------------------------------------*/
  //                          Characters Count
  /*----------------------------------------------------------------------------*/
  def char_count(inputFile: String, outputDir: String) {

    val sc = new SparkContext(new SparkConf().setAppName("Word Count"))
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    
    // split each document into words
    val tokenized = sc.textFile(inputFile)
 
    // count the occurrence of each word
    val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)
    wordCounts.foreach(println)

    // filter out words with less than threshold occurrences
    val filtered = wordCounts.filter(_._2 >= threshold)
 
    // count characters
    val charCounts = filtered.flatMap(_._1.toCharArray).map((_, 1)).reduceByKey(_ + _)
 
    //System.out.println(charCounts.collect().mkString(", "))
    println("=================================================")
    charCounts.foreach(println)
    println("=================================================")

  }

}


