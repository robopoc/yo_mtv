/**
  * Created by robo on 17/7/17.
  */
package yo

import java.security.InvalidParameterException
import java.sql.Timestamp

import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Dataset, Encoder, Encoders}
import sparkSession.implicits._

import scala.collection.mutable.ListBuffer

@InterfaceStability.Stable
class MultiSnapsFunctions[S <: EurexSnapshot](private val ds: Dataset[MultiSnapshot]) extends Serializable {

//  implicit val dd: Encoder[Prod] = ExpressionEncoder()
//  implicit val ss: Encoder[Snapshot] = ExpressionEncoder()

  val products: ListBuffer[String] = ListBuffer()

  def generate_snapshot_vector(products: ListBuffer[String]): Vector[(String,Option[EurexSnapshot])] = products.size match {
    case (l) if l < 2 => Vector()
    case _ => (products.head, None) +: generate_snapshot_vector(products.tail)
  }

  def addProduct(ps: (String, Snaps)): MultiSnaps = ps._2.count() match {
    case (0) => ds
    case _ => {
      products :+ ps._1
      products.size match {
        case (1) => {
          ps._2.map(s => MultiSnapshot(s.received, Vector((ps._1, Some(s)))))
        }
        case (l) => {
          val merged: Dataset[(MultiSnapshot, EurexSnapshot)] =
            ds.joinWith(ps._2, ds("received") === ps._2("received"), "outer")
          merged.map(m => m._1 match {
            case null => MultiSnapshot(m._2.received, generate_snapshot_vector(products) :+ ((ps._1, Some(m._2))))
            case _ => MultiSnapshot(m._1.received, m._1.products :+ ((ps._1, Some(m._2))))
          })
        }
      }
    }
  }

//  def scanPartition[S](z: S, f: (S,) => S): Dataset[S] = {
//    def scanner(i: Iterator[T]): Iterator[S] = {
//      val it = i.scanLeft(z)(f)
//      it.next()
//      it
//    }
//    ds.mapPartitions(scanner)
//  }
}

object MultiSnapsFunctions {
  implicit def addFunctions[S <: EurexSnapshot](ds: Dataset[MultiSnapshot]) = new MultiSnapsFunctions[S](ds)
}

//  def ks[S <: Snapshot](old: S, s: S): EurexSnapshot = {
//    EurexSnapshot(s.received,
//      Side[Bid](s.bids.qp() match {
//        case None => old.bids.quotes
//        case _ => s.bids.quotes},
//      s.bids.trades),
//      Side[Ask](s.asks.qp() match
//        case None => old.asks.quotes
//        case _ => s.asks.quotes},
//        s.asks.trades))
//  }
//
//  def keep_state(old: MultiSnapshot, ms: MultiSnapshot) = {
//    val ss : List[EurexSnapshot] = for {
//      s <- old.productMap.zip(ms.productMap)
//    } yield ks[EurexSnapshot](s._1,s._2)
//    MultiSnapshot(ms.received, ss)
//  }
//
//  def make_stateful(multiSnaps: MultiSnaps): MultiSnaps = {
//    scanPartition[MultiSnapshot,MultiSnapshot](multiSnaps)(new MultiSnapshot(new Timestamp(0L),List()), keep_state)
//  }
//}
