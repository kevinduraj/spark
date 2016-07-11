// Execute: 
// dse spark -i:count_disease_url.scala
import org.apache.spark.sql.functions._

val diseases=Array("mental","headache","cardio","disease","heart","fungal","gerson","vulgaris")

//-------------------------------------------------------------------------------------------------------------------------//
csc.setKeyspace("disease")
//csc.sql("CONSISTENCY ANY")

//-------------------------------------------------------------------------------------------------------------------------//
val df1 = csc.read.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "engine", "table" -> "visited" )).load

diseases.foreach( disease => { 
    val diseaseDF = df1.filter(col("url").contains(disease))
    diseaseDF.write.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "disease", "table" -> disease )).save
} )

//-------------------------------------------------------------------------------------------------------------------------//
// Count records
val df2 = csc.read.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "disease", "table" -> disease )).load
df2.count()
//-------------------------------------------------------------------------------------------------------------------------//

