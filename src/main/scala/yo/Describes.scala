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
  def received: Timestamp
  def bids: Side[Bid]
  def asks: Side[Ask]
  def side(bidAsk: BidAsk) = bidAsk match {
    case Bid() => bids
    case Ask() => asks
  }
  def midPrice(): Option[Double] = for (x <- side(Ask()).qp(); y <- side(Bid()).qp()) yield (x+y)*0.5
}

case class EurexSnapshot(received: java.sql.Timestamp, bids: Side[Bid], asks: Side[Ask]) extends Snapshot

case class MultiSnapshot(received: java.sql.Timestamp, products: Vector[(String,Option[EurexSnapshot])]) extends Serializable {
  def latest(): EurexSnapshot = products match {
    case last +: nil => last._2.get
    case Vector() => products.reduce((z1,z2) => (z1._2,z2._2) match {
      case (_,None) => z1
      case (None,Some(s)) => z2
      case (Some(s1),Some(s2)) => if (s1.received.after(s2.received)) z1 else z2
    })._2.get
    case _ => throw new NoSuchElementException("at least one event per row " + received)
  }
}

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