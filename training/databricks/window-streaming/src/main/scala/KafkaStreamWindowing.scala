
import com.datastax.spark.connector.SomeColumns
import kafka.serializer.StringDecoder

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import com.datastax.spark.connector.streaming._
import scala.util.Try

case class Trade (
stock_symbol:String,
exchange:String,
trade_timestamp: String,
price: Float,
quantity: Int)

case class Stats (stock_symbol: String,
      volume: Int,
      total_price: Float,
      high: Float,
      low: Float,
      average: Float,
      oldest: Float,
      oldest_timestamp: String,
      newest: Float,
      newest_timestamp: String,
      delta: Float,
      trades: Seq[String]) {

  def this(t:Trade) =
    this(stock_symbol = t.stock_symbol,
      volume       = t.quantity,
      total_price  = t.price,
      high         = t.price,
      low          = t.price,
      oldest       = t.price,
      oldest_timestamp = t.trade_timestamp,
      newest       = t.price,
      newest_timestamp = t.trade_timestamp,
      delta        = 0F,
      average      = t.price,
      trades = Seq(f"${t.quantity}%d@${t.price}%1.2f")
    )

  def munge(r:Stats) = Stats(
    stock_symbol  = stock_symbol,
    volume        = volume + r.volume,
    total_price   = total_price + r.total_price,
    high          = high max r.high,
    low           = low min r.low,
    oldest        = if (oldest_timestamp < r.oldest_timestamp) oldest else r.oldest,
    oldest_timestamp = if (oldest_timestamp < r.oldest_timestamp) oldest_timestamp else r.oldest_timestamp,
    newest        = if (newest_timestamp > r.newest_timestamp) newest else r.newest,
    newest_timestamp = if (newest_timestamp > r.newest_timestamp) newest_timestamp else r.newest_timestamp,
    delta         = newest - oldest,
    average       = (total_price + r.total_price) / (volume + r.volume),
    trades        = trades ++ r.trades)
}

//  %%cql create table if not exists stock.last_minute( stock_symbol text,
//    volume int,
//    high float,
//    low float,
//    average float,
//    delta float,
//    trades list<text>,
//    primary key (stock_symbol))


object KafkaStreamWindowing {

  def main(args: Array[String]) {


    // The batch interval sets how we collect data for, before analyzing it in a batch
    val batchInterval = Seconds(5)
    val windowInterval = Seconds(60)


    val conf = new SparkConf(true)
      .setAppName("KafkaStreamWindowing")

    val ssc = new StreamingContext(conf, Seconds(10))

    val directKafkaStream = KafkaUtils.createDirectStream[
      String, String, StringDecoder, StringDecoder](
        ssc, Map("metadata.broker.list" -> "kafka:9092"), Set("Trades"))

    val trades = directKafkaStream
      .map { case (tid, data)
    => data.split('|') match {
      case Array(ss, ex, dt, p, q)
      => Trade(ss, ex, dt, Try(p.toFloat).getOrElse(0F), Try(q.toInt).getOrElse(0))
      }
    }

    trades
      .map(t => (t.stock_symbol, new Stats(t)))
      .reduceByKeyAndWindow( _.munge(_) , windowInterval)
      .map(_._2)
      .saveToCassandra("stock", "last_10_seconds", SomeColumns("stock_symbol", "high", "low", "average", "volume", "delta","trades"))

    ssc.start

    ssc.awaitTermination()

  }

}
