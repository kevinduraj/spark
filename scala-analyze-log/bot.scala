// Build a model using Naiive Bayes
// spark-shell -i naive-bayes.scala

/*-------------------------------------------------------------------------------------------------------------------
  Validate convertsion from String to Integer
  -------------------------------------------------------------------------------------------------------------------*/
def validateSize(str: String): Integer = {
    var result=0
    try { result = str.toInt } 
    catch { case e: Exception => 0 }
    result
}
/*-------------------------------------------------------------------------------------------------------------------
    Read Httplog File
  -------------------------------------------------------------------------------------------------------------------*/
val file = "/var/log/httpd/access_log"
val lines = sc.textFile(file)
lines.cache()
case class HttpLog(http: String, size: Integer, ip: String, host: String, url: String, ref: String )

/*-------------------------------------------------------------------------------------------------------------------
    Analyze Bot visits
 -------------------------------------------------------------------------------------------------------------------*/

val botDF = lines.map(_.split("\\|")).filter(_(3).contains("bot")).map( r => HttpLog(r(0), validateSize(r(1)), r(2), r(3), r(4), r(5) ) ).toDF()
botDF.registerTempTable("bot_table")

val query3 = """SELECT host, COUNT(*) AS total FROM bot_table GROUP BY host ORDER BY total DESC LIMIT 25"""
sql(query3).show(20, false)

println("echo > " + file)

/*-------------------------------------------------------------------------------------------------------------------*/

exit()
