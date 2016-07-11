//----------------------------------------
//  dse spark -i:spark-update.scala
//----------------------------------------
import org.apache.spark.sql.functions._


//connecting to tables
csc.setKeyspace("emju_spark")

case class LeftTable(
      retail_customer_id: String, 
      household_id: Long, 
      customer_friendly_program_id: String,
      offer_id: Long,
      clip_id: Option[String],
      club_card_nbr: Long,
      last_update_user_id: String,
      offer_clip_ts: String
)

case class RightTable(
      retail_customer_id: String, 
      household_id: Long, 
      customer_friendly_program_id: String,
      offer_id: Long,
      clip_id: Option[String],
      club_card_nbr: Long,
      last_update_user_id: String,
      offer_clip_ts: String
)


val left  = sc.cassandraTable[LeftTable]("emju_spark", "clipped_offer_a_1m")
var right = left.map(a=> new RightTable(a.retail_customer_id, 
                                        a.household_id, 
                                        a.customer_friendly_program_id, 
                                        a.offer_id,
                                        a.clip_id, 
                                        a.club_card_nbr+1000000,
                                        a.last_update_user_id,
                                        a.offer_clip_ts))

right.foreach(println)
right.saveToCassandra("emju_spark", "clipped_offer_b_1m")

sys.exit()
