//-------------------------------------------------------------------------------------------------------------------------//
// Execute: Spark Job 
// dse spark -i:save-filter-table.scala
// dse hadoop fs -ls /user/cassandra
// dse hadoop fs -getmerge /user/root/tumor tumor.dat
// dse hadoop fs -rmr /user/root/tumor
//-------------------------------------------------------------------------------------------------------------------------//
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SQLContext}
//-------------------------------------------------------------------------------------------------------------------------//
//cat selected | awk  '{ print ",\x22"$1"\x22" }' | tee -a ../save-filter-table.scala 
//-------------------------------------------------------------------------------------------------------------------------//
val diseases=Array("virus","visual","vitamin","vulgaris")
 
println(diseases)

//-------------------------------------------------------------------------------------------------------------------------//
// Read Table from Keyspace into Data Frame
csc.setKeyspace("disease")
val df1 = csc.read.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "cloud4", "table" -> "link3" )).load
df1.printSchema()
//-------------------------------------------------------------------------------------------------------------------------//
diseases.foreach( disease => {

    println(disease)
    val df2 = df1.filter(col("url").contains(disease))
    val df3 = df2.withColumn("condition", org.apache.spark.sql.functions.lit("url"))
    df3.write.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "cloud4", "table" -> "temp" )).mode(SaveMode.Overwrite).save()
    csc.sql("INSERT into table diseases SELECT * from temp")
} )

//-------------------------------------------------------------------------------------------------------------------------//
// Count records
//val df4 = csc.read.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "disease", "table" -> "general" )).load
//df4.count()
//-------------------------------------------------------------------------------------------------------------------------//

