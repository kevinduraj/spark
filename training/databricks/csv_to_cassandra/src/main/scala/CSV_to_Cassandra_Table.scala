import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object CSV_to_Cassandra_Table {

  def main(args: Array[String]) {

	//    Create Spark Context
	val conf = new SparkConf(true).setAppName("CSV_to_Cassandra_Table")

	// We set master on the command line for flexibility

	val sc = new SparkContext(conf)
	val sqlContext = new HiveContext(sc)

	val clickstreamDF = sqlContext.read.format("com.databricks.spark.csv")
	  .option("header", "true")
	  .option("delimiter", ",")
	  .option("mode", "PERMISSIVE")
	  .option("inferSchema", "true")
	  .load("file:///mnt/ephemeral/summitdata/tracks_by_album.csv")

	clickstreamDF.count()

	clickstreamDF.printSchema()

	val renamed = clickstreamDF
			.withColumnRenamed("album", "album_title")
			.withColumnRenamed("year", "album_year")
			.withColumnRenamed("genre", "album_genre")
			.withColumnRenamed("number", "track_number")
			.withColumnRenamed("title", "track_title")
			

	renamed.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "tracks_by_album", "keyspace" -> "musiccsv")).save()
    }
}
	

// select count(*) from musiccsv.tracks_by_album;


