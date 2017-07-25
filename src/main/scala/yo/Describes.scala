/**
  * Created by robo on 17/7/17.
  */
package yo

import java.time.ZonedDateTime

class Product
case class FDAX() extends Product
case class FSTX() extends Product

class BidAsk
case class Bid() extends BidAsk
case class Ask() extends BidAsk

case class PriceVolume(tickPrice: Long, quotedVolume: Long)
case class Side(side: BidAsk, quotes: List[PriceVolume], trades: List[PriceVolume])

trait Snapshot {
  def received: ZonedDateTime
  def bids: Side
  def asks: Side
}
case class EurexSnapshot(received: ZonedDateTime, bids: Side, asks: Side) extends Snapshot

case class MultiSnapshot(received: ZonedDateTime, productMap: Map[Product,Snapshot])


