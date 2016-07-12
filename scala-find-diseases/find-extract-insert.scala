/*------------------------------------------------------------------------------------------------------------------------
 dse spark -i:find-extract-insert.scala
 --------------------------------------------------
 dse hadoop fs -ls /user/cassandra
 dse hadoop fs -getmerge /user/root/tumor tumor.dat
 dse hadoop fs -rmr /user/root/tumor
 https://spark.apache.org/docs/1.4.0/api/java/org/apache/spark/sql/SaveMode.html
-------------------------------------------------------------------------------------------------------------------------*/
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SQLContext}
//-------------------------------------------------------------------------------------------------------------------------//
//cat selected | awk  '{ print ",\x22"$1"\x22" }' | tee -a ../save-filter-table.scala 
//-------------------------------------------------------------------------------------------------------------------------//
val diseases=Array(
"allergy"
,"anemia"
,"autism"
,"bacteria"
,"biology"
,"brain"    
,"cancer"  
,"cardio"   
,"disease"  
,"disorder"
,"drug"
,"fever"
,"gastric"
,"genetic"
,"gerson"  
,"head" 
,"health"  
,"heart"    
,"intestin"
,"kidney"
,"liver"
,"lung"
,"lyme"
,"mental"   
,"nasal"
,"pain"
,"parkinsons"
,"pulmonary"
,"renal"
,"respiratory"
,"sickness"
,"skin"
,"spinal"
,"surgery"
,"symptoms"
,"syndrome"
,"tissue"
,"treatment"
,"tuberculosis"
,"tumor"
,"vascular"
,"virus"
,"organic"
,"vitamin"
)
 
println(diseases)

//-------------------------------------------------------------------------------------------------------------------------//
csc.setKeyspace("cloud4")
val df1 = csc.read.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "cloud4", "table" -> "link3" )).load
df1.printSchema()
//-------------------------------------------------------------------------------------------------------------------------//
diseases.foreach( disease => {

    println(disease)

    val df2 = df1.filter(col("url").contains(disease))
    val df3 = df2.withColumn("condition", org.apache.spark.sql.functions.lit(disease))

    //df3.write.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "cloud1", "table" -> "temp" )).mode(SaveMode.Overwrite).save()
    //csc.sql("INSERT into table cloud1.diseases SELECT * from cloud1.temp")

    df3.write.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "cloud1", "table" -> "diseases" )).mode(SaveMode.Append).save()
    
} )

//-------------------------------------------------------------------------------------------------------------------------//
// Count records
//val df4 = csc.read.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "disease", "table" -> "general" )).load
//df4.count()
//-------------------------------------------------------------------------------------------------------------------------//

