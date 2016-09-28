/*-------------------------------------------------------------*/
// dse spark -i:export-health-links.scala
/*-------------------------------------------------------------*/
import org.apache.spark.sql.functions._

csc.setKeyspace("cloud4")

val QUERY = """SELECT url FROM cloud4.link3 WHERE url LIKE '%heart%'        UNION 
               SELECT url FROM cloud4.link3 WHERE url LIKE '%cancer%'       UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%chronic%'      UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%respiratory%'  UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%disease%'      UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%stroke%'       UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%alzheimer%'    UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%diabetes%'     UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%influenza%'    UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%pneumonia%'    UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%pulmonary%'    UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%lung%'         UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%tuberculosis%' UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%virus%'        UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%nutrition%'    UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%organic%'      UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%enema%'        UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%medicine%'     UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%prescription%' UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%painkillers%'  UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%depress%'      UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%gerson%'       UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%health%'       UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%drug%'         UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%genetic%'      UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%nervous%'      UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%tumor%'        UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%surgeon%'      UNION
               SELECT url FROM cloud4.link3 WHERE url LIKE '%organic%'"""

val df1 = csc.sql(QUERY).toDF()
val rdd2 = df1.rdd
rdd2.saveAsTextFile("health6.dat")

sys.exit
/*-------------------------------------------------------------*/
