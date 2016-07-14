
import _root_.kafka.serializer.StringDecoder

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import scala.util.Try
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector._



//  %%cql create table if not exists stock.last_minute( stock_symbol text,
//    volume int,
//    high float,
//    low float,
//    average float,
//    delta float,
//    trades list<text>,
//    primary key (stock_symbol))


object PortfolioStreaming {


    case class Trade (
                       stock_symbol:String,
                       exchange:String,
                       trade_timestamp: String,
                       price: Float,
                       quantity: Int)

    case class Portfolio (
                           name: String,
                           stock_symbol: String,
                           quantity: Int,
                           price: Option[Float],
                           value: Option[Float]
                           )

  def main(args: Array[String]) {


    // The batch interval sets how we collect data for, before analyzing it in a batch

    val BatchInterval = Seconds(5)

    val conf = new SparkConf(true)
      .setAppName("PortfolioStream")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, BatchInterval)

    val directKafkaStream = KafkaUtils.createDirectStream[
      String, String, StringDecoder, StringDecoder ](
        ssc, Map("metadata.broker.list" ->"kafka:9092"), Set("Trades"))

    // turn the DStream of foo|bar|baz strings to Trades

    val trades = directKafkaStream
      .map{ case (tid, data)
              => data.split('|') match {
                case Array(ss,ex,dt,p,q)
                    => Trade(ss,ex,dt,Try(p.toFloat).getOrElse(0F),Try(q.toInt).getOrElse(0))
              }
          }

    val portfolios = ssc.cassandraTable[Portfolio]("stock","portfolios").keyBy[String]("stock_symbol")

// TODO:
//    For each batch,
//      You can choose to use either RDDs, SparkSQL, or DataFrames
//
//      get the newest trade for each symbol. RDD Hint: Use a reduce for this
//      join it to portfolios
//      set the price for the item to the new price
//      set the value for the item to the new price * quantity
//      save it to the porfolios table


// TODO: Start the Job

    ssc.awaitTermination()

  }

}
