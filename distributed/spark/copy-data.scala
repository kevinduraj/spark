//-----------------------------------------------------------------------------------------//
// dse spark -i:copy-data.scala
//-----------------------------------------------------------------------------------------//
import org.apache.spark.sql.functions._

//-----------------------------------------------------------------------------------------//
//                        Copy Data Between Keyspaces                                      //
//-----------------------------------------------------------------------------------------//

csc.sql("INSERT INTO TABLE cloud4.vdomain    SELECT * FROM cloud2.vdomain   ").collect()
csc.sql("INSERT INTO TABLE cloud4.ldomain    SELECT * FROM cloud2.ldomain   ").collect()
//csc.sql("INSERT INTO TABLE cloud4.visit      SELECT * FROM cloud2.visit     ").collect()
csc.sql("INSERT INTO TABLE cloud4.link1      SELECT * FROM cloud2.link1     ").collect()
csc.sql("INSERT INTO TABLE cloud4.lfacebook  SELECT * FROM cloud2.lfacebook ").collect()
csc.sql("INSERT INTO TABLE cloud4.llinkedin  SELECT * FROM cloud2.llinkedin ").collect()
csc.sql("INSERT INTO TABLE cloud4.ltwitter   SELECT * FROM cloud2.ltwitter  ").collect()
csc.sql("INSERT INTO TABLE cloud4.lyoutube   SELECT * FROM cloud2.lyoutube  ").collect()
csc.sql("INSERT INTO TABLE cloud4.lwordpress SELECT * FROM cloud2.lwordpress").collect()
csc.sql("INSERT INTO TABLE cloud4.linstagram SELECT * FROM cloud2.linstagram").collect()

//-----------------------------------------------------------------------------------------//

sys.exit()
