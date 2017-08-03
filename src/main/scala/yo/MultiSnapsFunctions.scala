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
import DatasetFunctions._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

@InterfaceStability.Stable
class MultiSnapsFunctions[S <: EurexSnapshot](private val ds: Dataset[MultiSnapshot]) extends Serializable {

//  implicit val dd: Encoder[Prod] = ExpressionEncoder()
//  implicit val ss: Encoder[Snapshot] = ExpressionEncoder()

  val products: mutable.LinkedHashMap[String,(Int,ZoneId)] = mutable.LinkedHashMap()
  val tradedProducts: ListBuffer[String] = ListBuffer()
  var timezone: Option[ZoneId] = None //ZoneId.of("Europe/Zurich")

  def generate_snapshot_vector(products: Iterable[String]): Vector[(String,Option[EurexSnapshot])] = products.size match {
    case (0) => Vector()
    case _ => (products.head, None) +: generate_snapshot_vector(products.tail)
  }

  def addProduct(ps: (String, Snaps)): MultiSnaps = ps._2.count() match {
    case (0) => ds
    case _ => {
      products + ((ps._1,products.size))
      val new_ds = products.size match {
        case (1) => {
          ps._2.map(s => MultiSnapshot(s.received, s.ssd, Vector((ps._1, Some(s)))))
        }
        case (l) => {
          val merged: Dataset[(MultiSnapshot, EurexSnapshot)] =
            ds.joinWith(ps._2, ds("received") === ps._2("received"), "outer")
          merged.map(m => m._1 match {
            case null => MultiSnapshot(m._2.received, m._2.ssd, generate_snapshot_vector(products.dropRight(1).keys) :+ ((ps._1, Some(m._2))))
            case _ => MultiSnapshot(m._1.received, m._1.ssd, m._1.products :+ ((ps._1, Some(m._2))))
          })
        }
      }
      new_ds.repartition(new_ds.groupBy(new_ds("ssd")).count().count().toInt,new_ds("ssd"))
    }
  }

  def fill(): MultiSnaps = {
    def ff(ii: Iterator[MultiSnapshot]): Iterator[MultiSnapshot] = {
      ii.scanLeft[MultiSnapshot](MultiSnapshot(0L, 0, generate_snapshot_vector(products.keys)))(_ fill _).filter(r => r.received != 0L)
    }
    ds.mapPartitions(ff)
  }

  def isTradedProduct(product: String): Unit = {
    if (products.contains(product)) {
      tradedProducts + product
      timezone = Some(products(product)._2)
    }
    else throw new InvalidParameterException(product + " is not contained in the MultiSnaps")
  }

}

object MultiSnapsFunctions {
  implicit def addFunctions[S <: EurexSnapshot](ds: Dataset[MultiSnapshot]) = new MultiSnapsFunctions[S](ds)

  def createMultiSnaps(products: List[(String, Snaps)]) = products match {
    case null => sparkSession.emptyDataset[MultiSnapshot]
    case Nil => sparkSession.emptyDataset[MultiSnapshot]
    case _ => products.foldLeft[MultiSnaps](sparkSession.emptyDataset[MultiSnapshot])(_ addProduct _)
  }
}
