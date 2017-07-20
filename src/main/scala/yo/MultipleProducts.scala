/**
  * Created by robo on 17/7/17.
  */
package yo

import java.security.InvalidParameterException
import org.apache.spark.sql.{Dataset, SparkSession}


class MultipleProducts() {
  val sparkSession = SparkSession.builder.appName("multi product test suite").master("local[*]").getOrCreate()
  import sparkSession.implicits._

  def combine_products(productMap: Map[Product,Dataset[RetailState]]): Dataset[Snapshot] = productMap.size match {
    case (0) => throw new InvalidParameterException("At least 1 product has to be contained in the product map")
    case (1) => {
      productMap.toList(0)._2.map(rs => Snapshot(rs.received,Map()(productMap.toList(0)._1,rs)))
    }
    case (l) if (l >= 2) => {
      val tail_merge: Dataset[Snapshot] = combine_products(productMap.tail)
      val merged : Dataset[(Snapshot,RetailState)] = tail_merge.joinWith(
        productMap.head._2, tail_merge.col("received") === productMap.head._2.col("received"), "outer")
      merged.map((s) => {
        if (s._1.productMap.isEmpty) Snapshot(s._2.received, Map()(productMap.head._1,s._2))
        else Snapshot(s._1.received, s._1.productMap.+((productMap.head._1,s._2)))
      })
    }
  }
}
