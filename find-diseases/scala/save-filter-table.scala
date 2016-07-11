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
val diseases=Array(
"abscess"
,"allergy"
,"anemia"
,"angiomatosis"
,"appendicitis"
,"aspergillosis"
,"autism"
,"bacillary"
,"bacterial"
,"biochemical"
,"biology"
,"bladder"
,"bone"
,"brain"    
,"bronchitis"
,"cancer"
,"cancer"  
,"carcinoma"
,"cardio"   
,"cervical"
,"chlamydial"
,"choriomeningitis"
,"chromozone"
,"chronic"
,"collagen"
,"congenita"
,"contagious"
,"cortical"
,"deafness"
,"dental"
,"diabetic"
,"disease"  
,"disorder"
,"dissection"
,"drug"
,"emboli"
,"fatty"
,"fever"
,"fungal"   
,"gastric"
,"gastrointestinal"
,"genetic"
,"genetics"
,"gerson"  
,"goitre"
,"gout"
,"granulomatosis"
,"headache" 
,"health"  
,"heart"    
,"heatstroke"
,"hematologic"
,"hemoglobinopathies"
,"hemorrhagic"
,"hepatitis"
,"hormonal"
,"hutchinson"
,"hyperactivity"
,"hypertension"
,"intestinal"
,"intravascular"
,"kidney"
,"liver"
,"lung"
,"lyme"
,"lymphocytic"
,"lymphomatoid"
,"meningococcal"
,"mental"   
,"metabolic"
,"mycoplasmal"
,"myofascial"
,"nasal"
,"necrosis"
,"nematodes"
,"osteoporosis"
,"overdose"
,"pain"
,"paraneoplastic"
,"parkinsons"
,"phenomena"
,"prednisone"
,"prostatic"
,"pulmonary"
,"renal"
,"respiratory"
,"salmonellosis"
,"schizoaffective"
,"schizophrenia"
,"sickness"
,"skin"
,"sleeping"
,"spinal"
,"surgery"
,"symptoms"
,"syndrome"
,"tissue"
,"treatment"
,"tuberculosis"
,"tuboovaria"
,"tumor"
,"urticaria"
,"vascular"
,"virus"
,"visual"
,"vitamin"
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
    df3.write.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "disease", "table" -> "temp" )).mode(SaveMode.Overwrite).save()
    csc.sql("INSERT into table general SELECT * from temp")
} )

//-------------------------------------------------------------------------------------------------------------------------//
// Count records
//val df4 = csc.read.format("org.apache.spark.sql.cassandra").options(Map( "keyspace" -> "disease", "table" -> "general" )).load
//df4.count()
//-------------------------------------------------------------------------------------------------------------------------//

