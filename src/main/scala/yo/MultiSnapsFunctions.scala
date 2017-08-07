/**
  * Created by robo on 17/7/17.
  */
package yo

import java.security.InvalidParameterException
import java.sql.Timestamp
import java.time.ZoneId

import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Dataset, Encoder, Encoders}
import sparkSession.implicits._
import SnapsFunctions._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class MultiSnapsFunctions(private val ds: MultiSnaps) extends Serializable {

//  implicit val dd: Encoder[Prod] = ExpressionEncoder()
//  implicit val ss: Encoder[Snapshot] = ExpressionEncoder()

  //var products: mutable.LinkedHashMap[String,(Int,ZoneId)] = mutable.LinkedHashMap()
  //val tradedProducts: ListBuffer[String] = ListBuffer()
  //var timezone: Option[ZoneId] = None //ZoneId.of("Europe/Zurich")§

//  def addProduct(ps: (String, Snaps)): MultiSnaps = ps._2.count() match {
//    case (0) => ds
//    case _ => {
//      //products += ((ps._1,(products.size, ZoneId.of("Europe/Zurich"))))
//      val mm = MultiSnapsFunctions.addFunctions(products.size match {
//        case (1) => {
//          ps._2.map(s => MultiSnapshot(s.received, s.ssd, Vector((ps._1, Some(s)))))
//        }
//        case (l) => {
//          val merged: Dataset[(MultiSnapshot, EurexSnapshot)] = ds.joinWith(ps._2, ds("received") === ps._2("received"), "outer")
//          merged.map(m => m._1 match {
//            case null => MultiSnapshot(m._2.received, m._2.ssd, MultiSnapsFunctions.generate_snapshot_vector(products.dropRight(1).keys) :+ ((ps._1, Some(m._2))))
//            case _ => MultiSnapshot(m._1.received, m._1.ssd, m._1.products :+ ((ps._1, Some(m._2))))
//          })
//        }
//      })
//      mm.products += ((ps._1,(products.size, ZoneId.of("Europe/Zurich"))))
//      mm.fill()
//    }
//  }

  def fill(): MultiSnaps = {
    def ff(prods: Iterable[String])(ii: Iterator[MultiSnapshot]): Iterator[MultiSnapshot] = {
      ii.scanLeft[MultiSnapshot](MultiSnapshot(0L, 0, MultiSnapsFunctions.generate_snapshot_vector(prods)))(_ fill _).filter(r => r.received != 0L)
    }
    ds.mapPartitions(ff(ds.take(1).head.products.map(p => p._1)))
  }

//  def isTradedProduct(product: String): Unit = {
//    if (products.contains(product)) {
//      tradedProducts + product
//      timezone = Some(products(product)._2)
//    }
//    else throw new InvalidParameterException(product + " is not contained in the MultiSnaps")
//  }

}

object MultiSnapsFunctions {
  implicit def addFunctions(ds: Dataset[MultiSnapshot]) = new MultiSnapsFunctions(ds)

  def empty() = sparkSession.emptyDataset[MultiSnapshot]

  def generate_snapshot_vector(products: Iterable[String]): Vector[(String,Option[EurexSnapshot])] = products.size match {
    case (0) => Vector()
    case _ => (products.head, None) +: generate_snapshot_vector(products.tail)
  }

  def combine(multiSnaps1: MultiSnaps, multiSnaps2: MultiSnaps): MultiSnaps = {
    val merged: Dataset[(MultiSnapshot, MultiSnapshot)] = multiSnaps1.joinWith(multiSnaps2,
      multiSnaps1("received") === multiSnaps2("received"), "outer")
    val p1 = multiSnaps1.count() match {
      case 0 => Vector()
      case _ => generate_snapshot_vector(multiSnaps1.take(1).head.products.map(p => p._1))
    }
    val p2 = multiSnaps2.count() match {
      case 0 => Vector()
      case _ => generate_snapshot_vector(multiSnaps2.take(1).head.products.map(p => p._1))
    }
    val mm = merged.map(m => m match {
      case (null,m2) => MultiSnapshot(m._2.received, m._2.ssd,
        p1 ++ m._2.products)
      case (m1,null) => MultiSnapshot(m._1.received, m._1.ssd,
        m._1.products ++ p2)
      case (m1,m2) => MultiSnapshot(m._1.received, m._1.ssd,
        m._1.products ++ m._2.products)
    })
    mm.repartition(Math.max(1,mm.select("ssd").distinct().count().toInt),mm("ssd"))
  }

  def createMultiSnaps(products: List[(String, Snaps)]) = {
    products.map(p => p._2.toMultiSnaps(p._1)).fold(empty())(combine).sort()
  }
//    case null => empty
//    case Nil => empty
//    case _ => {
//      val ms: MultiSnaps = products.foldLeft[MultiSnaps](empty)(_ addProduct _)
//      val m = mutable.LinkedHashMap.newBuilder
//      m.
//      ms.products =    .map(l => (l._1,ZoneId.of("Europe/Amsterdam"))).zipWithIndex.map(r => (r._1._1,(r._2,r._1._2))).toMap[mutable.LinkedHashMap[String,(Int,ZoneId)]]
//    }
//  }
}
