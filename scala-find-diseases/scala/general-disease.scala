/*---------------------------------------------*/
// Execute: Spark Job 
// dse spark -i:general-disease.scala
/*---------------------------------------------*/
import org.apache.spark.sql.functions._

val diseases=Array("mental","headache","cardio","disease","heart","fungal","gerson","vulgaris")

//-------------------------------------------------------------------------------------------------------------------------//
csc.setKeyspace("cloud4")

//-------------------------------------------------------------------------------------------------------------------------//
val df1 = csc.read.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "cloud4", "table" -> "link3" )).load

diseases.foreach( disease => { 
    val diseaseDF = df1.filter(col("url").contains(disease))
    diseaseDF.write.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "cloud4", "table" -> "link_clean" )).save
} )

//-------------------------------------------------------------------------------------------------------------------------//
// Count records
val df2 = csc.read.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "cloud4", "table" -> "link_clean" )).load
df2.count()
//-------------------------------------------------------------------------------------------------------------------------//

