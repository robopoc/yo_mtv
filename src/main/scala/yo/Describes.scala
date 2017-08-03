/**
  * Created by robo on 17/7/17.
  */
package yo

import java.sql.Timestamp

import org.apache.spark.sql.{Encoder, Encoders}

trait Prod {
  def name: String
}
case class FDAX(name:String = "FDAX") extends Prod
case class FESX(name:String = "FESX") extends Prod

class BidAsk
case class Bid() extends BidAsk
case class Ask() extends BidAsk

case class PriceVolume(tickPrice: Long, deltaVolume: Long)
case class Side[S <: BidAsk](quotes: Vector[PriceVolume], trades: Vector[PriceVolume]) {
  def qp(level: Int = 0): Option[Long] = if (quotes.size <= level) None else Some(quotes(level).tickPrice)
  def qv(level: Int = 0): Option[Long] = if (quotes.size <= level) None else Some(quotes(level).deltaVolume)
  def tp(level: Int = 0): Option[Long] = if (quotes.size <= level) None else Some(quotes(level).tickPrice)
  def tv(level: Int = 0): Option[Long] = if (quotes.size <= level) None else Some(quotes(level).deltaVolume)
}

abstract class Snapshot {
  def received: Long
  def ssd: Int
  def bids: Side[Bid]
  def asks: Side[Ask]
  def side(bidAsk: BidAsk) = bidAsk match {
    case Bid() => bids
    case Ask() => asks
  }
  def midPrice(): Option[Double] = for (x <- side(Ask()).qp(); y <- side(Bid()).qp()) yield (x+y)*0.5

  def fill(other: Option[EurexSnapshot]): Option[EurexSnapshot]
}

case class EurexSnapshot(received: Long, ssd: Int, bids: Side[Bid], asks: Side[Ask]) extends Snapshot {
  override def fill(other: Option[EurexSnapshot]) = other match {
    case None => Some(this.copy())
    case Some(s) => Some(EurexSnapshot(s.received,
      s.ssd,
      Side[Bid](bids.qp() match {
        case None => s.bids.quotes
        case _ => bids.quotes
      },
        s.bids.trades),
      Side[Ask](asks.qp() match {
        case None => asks.quotes
        case _ => s.asks.quotes
      },
        s.asks.trades)))
  }
}

case class MultiSnapshot(received: Long, ssd: Int,
                         products: Vector[(String,Option[EurexSnapshot])]) extends Serializable {
  def latest(): EurexSnapshot = products match {
    case last +: nil => last._2.get
    case Vector() => products.reduce((z1,z2) => (z1._2,z2._2) match {
      case (_,None) => z1
      case (None,Some(s)) => z2
      case (Some(s1),Some(s2)) => if (s1.received > s2.received) z1 else z2
    })._2.get
    case _ => throw new NoSuchElementException("at least one event per row " + received)
  }

  def fill(other: MultiSnapshot) = {
    val sss : Vector[(String,Option[EurexSnapshot])] = for {
      s <- other.products.zip(products)
      ss = s._2._2 match {
        case None => s._1._2
        case Some(es) => es.fill(s._1._2)
      }
    } yield (s._1._1,ss)
    MultiSnapshot(received, ssd, sss)
  }
}

case class PVS(price: Double, volume: Double, Source: String)
case class Avros(ts: Long, feedcode: String, bid: Vector[(Long,Double)], ask: Vector[(Long,Double)], tick: PVS)

//object Encooders {
//  implicit def dd: Encoder[Prod] = Encoders.kryo[Prod]
//  implicit def fdaxEnc : Encoder[FDAX] = Encoders.product[FDAX]
//  implicit def fesxEnc : Encoder[FESX] = Encoders.product[FESX]
//  implicit def pvEnc : Encoder[PriceVolume] = Encoders.product[PriceVolume]
//  implicit def bidEnc : Encoder[Side[Bid]] = Encoders.product[Side[Bid]]
//  implicit def askEnc : Encoder[Side[Ask]] = Encoders.product[Side[Ask]]
//  //implicit def snapEnc : Encoder[Snapshot] = Encoders.product[Snapshot]
//  implicit def esEnc : Encoder[EurexSnapshot] = Encoders.product[EurexSnapshot]
//  implicit def msEnc : Encoder[MultiSnapshot] = Encoders.product[MultiSnapshot]
//}