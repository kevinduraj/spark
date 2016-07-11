//-------------------------------------------------------------------------------------------------------------------------//
// Execute: Spark Job 
// dse spark -i:partition-general-disease.scala 
// dse hadoop fs -ls /user/cassandra
// dse hadoop fs -getmerge /user/root/tumor tumor.dat
// dse hadoop fs -rmr /user/root/tumor
//-------------------------------------------------------------------------------------------------------------------------//
import org.apache.spark.sql.functions._

val diseases=Array(
"carcinoma"
,"treatment"
,"syndrome"
,"tumor"
,"liver"
,"lung"
,"gastric"
,"tuberculosis"
,"surgery"
,"genetics"
,"chromozone"
,"health"  
,"cancer"  
,"hepatitis"
,"brain"    
,"biology"
,"mental"   
,"headache" 
,"cardio"   
,"disease"  
,"heart"    
,"fungal"   
,"gerson"  
,"vulgaris"
)
 
println(diseases)

//-------------------------------------------------------------------------------------------------------------------------//
// Read Table from Keyspace into Data Frame
csc.setKeyspace("disease")
val df1 = csc.read.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "engine", "table" -> "visited" )).load
df1.printSchema()
//-------------------------------------------------------------------------------------------------------------------------//
diseases.foreach( disease => {

    println(disease)
    val df2 = df1.filter(col("url").contains(disease))
    val df3 = df2.withColumn("condition", org.apache.spark.sql.functions.lit(disease))
    df3.rdd.coalesce(1,true).saveAsTextFile(disease) 

} )

//-------------------------------------------------------------------------------------------------------------------------//
// Count records
//val df4 = csc.read.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "disease", "table" -> "general" )).load
//df4.count()
//-------------------------------------------------------------------------------------------------------------------------//

