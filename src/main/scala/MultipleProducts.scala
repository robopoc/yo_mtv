/**
  * Created by robo on 17/7/17.
  */
package yo

import com.cloudera.sparkts.DateTimeIndex
import org.apache.spark.sql.Dataset

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

class MultipleProducts {
  val x: DateTimeIndex = DateTimeIndex.irregular(Array(1L,2L,3L))

  class Product
  case class FDAX() extends Product
  class BidAsk
  case class Bid() extends BidAsk
  case class Ask() extends BidAsk
  case class PriceVolume(tickPrice: Long, quotedVolume: Long)
  case class Side(side: BidAsk, quotes: List[PriceVolume], trades: List[PriceVolume])
  case class RetailState(received: DateTimeIndex, bids: Side, asks: Side)
  case class Snapshot(received: DateTimeIndex, productMap: Map[Product,RetailState])

  val ds: Dataset[Snapshot] = ???

  val conf = new SparkConf().setAppName("fuck").setMaster("local[*]")
  val sc = new SparkContext(conf)

  def merge_products(productMap: Map[Product,Dataset[RetailState]]): Dataset[Snapshot] = productMap.size match {
    case (l) if (l == 0) => {
      val ss: Seq[Snapshot] = Seq[Snapshot]()
      ss.toDS()
    }
    case (l) if (l == 1) => productMap.toList(0)._2.map(rs => Snapshot(rs.received,))
    case (l) if (l >= 2) => {
      val tail_merge: Dataset[Snapshot] = merge_products(productMap.tail)
      val merged : Dataset[(Snapshot,RetailState)] = tail_merge.joinWith(productMap.head._2, tail_merge.col("received") === productMap.head._2.col("received"), "outer")
      merged.map((s) => Snapshot(if (s._1.productMap.isEmpty) s._2.received else s._1.received, s._1.productMap + s._2)
      })
    }
  }


}
