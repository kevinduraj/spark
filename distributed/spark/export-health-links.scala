/*-------------------------------------------------------------*/
// dse spark -i:export-health-links.scala
/*-------------------------------------------------------------*/
import org.apache.spark.sql.functions._

csc.setKeyspace("cloud4")

val QUERY = """SELECT url FROM cloud4.link3 WHERE url LIKE '%heart%'        UNION 
               SELECT url FROM cloud4.link3 WHERE url LIKE '%cancer%'       UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%chronic%'      UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%disease%'      UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%alzheimer%'    UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%diabetes%'     UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%virus%'        UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%nutrition%'    UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%organic%'      UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%medicine%'     UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%health%'       UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%tumor%'"""


val df1 = csc.sql(QUERY).toDF()
val rdd2 = df1.rdd
rdd2.saveAsTextFile("health7.dat")

sys.exit
/*-------------------------------------------------------------*/
