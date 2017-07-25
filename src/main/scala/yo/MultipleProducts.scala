/**
  * Created by robo on 17/7/17.
  */
package yo

import java.security.InvalidParameterException

import org.apache.spark.sql.Dataset
import sparkSession.implicits._

class MultipleProducts() {

  def combine_products[S <: Snapshot](productMap: Map[Product,Snaps[S]]): MultiSnaps = productMap.size match {
    case (0) => throw new InvalidParameterException("At least 1 product has to be contained in the product map")
    case (1) => {
      productMap.toList(0)._2.map(rs => MultiSnapshot(rs.received,Map()(productMap.toList(0)._1,rs)))
    }
    case (l) if (l >= 2) => {
      val tail_merge: MultiSnaps = combine_products(productMap.tail)
      val merged : Dataset[(MultiSnapshot,S)] = tail_merge.joinWith(
        productMap.head._2, tail_merge.col("received") === productMap.head._2.col("received"), "outer")
      merged.map((s) => {
        if (s._1.productMap.isEmpty) MultiSnapshot(s._2.received, Map()(productMap.head._1,s._2))
        else MultiSnapshot(s._1.received, s._1.productMap.+((productMap.head._1,s._2)))
      })
    }
  }

  val ddd = combine_products(Map())
  ddd.toDF()
}
