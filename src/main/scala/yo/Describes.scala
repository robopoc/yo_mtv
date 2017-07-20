package yo

import java.time.ZonedDateTime

/**
  * Created by robo on 17/7/17.
  */
class Product
case class FDAX() extends Product
case class FSTX() extends Product
class BidAsk
case class Bid() extends BidAsk
case class Ask() extends BidAsk
case class PriceVolume(tickPrice: Long, quotedVolume: Long)
case class Side(side: BidAsk, quotes: List[PriceVolume], trades: List[PriceVolume])
case class RetailState(received: ZonedDateTime, bids: Side, asks: Side)
case class Snapshot(received: ZonedDateTime, productMap: Map[Product,RetailState])


