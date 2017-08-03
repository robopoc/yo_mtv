package yo

import java.security.InvalidParameterException
import java.sql.Date
import java.time.ZoneId
import com.databricks.spark.avro._

import org.apache.spark.annotation.InterfaceStability
import sparkSession.implicits._
/**
  * Created by robo on 28/7/17.
  */
//@InterfaceStability.Stable
class SnapsFunctions(private val ds: Snaps) extends Serializable {
//  def toMultiSnaps(product: Prod): MultiSnaps = ds.count() match {
//    case (0) => throw new InvalidParameterException("Won't create MultiSnaps from empty Snaps")
//    case _ => ds.map(s => MultiSnapshot(s.received, Map((product, s))))
//  }
  var product: Option[(String, ZoneId)] = None
}

//
object SnapsFunctions {
  implicit def addFunctions(ds: Snaps) = new SnapsFunctions(ds)

  def fromAvro(product: (String,Map[Date,String]), days: Iterable[Date]): Snaps = {
    val ds = sparkSession.read.avro("/Users/robo/data/avro_test.avro").as[Avros]
    val ds_prod = ds.filter(av => av.feedcode.contains("FUT_NK225_"))

    def trades(tick: PVS, bid: Vector[(Long,Double)], ask: Vector[(Long,Double)]): Option[(BidAsk, Vector[PriceVolume])] = {
      val side = tick match {
        case null => None
        case _ => tick.Source match {
          case "TICK_NORMAL" => {
            if (!bid.isEmpty && bid.head._2 == tick.price) Some(Bid)
            else if (!ask.isEmpty && ask.head._2 == tick.price) Some(Ask)
            else None
          }
          case _ => None
        }
      }
      side match {
        case None => None
        case Some(s) => (s,Vector(PriceVolume(Math.round(tick.price), Math.round(tick.volume))))
    }

    ds_prod.map(av => {
      val tr = trades(av.tick, av.bid, av.ask)
      tr match {
        case Some((Bid,s)) => EurexSnapshot(av.ts, Side[Bid](av.bid, s), Side[Ask](av.ask, Vector()))
        case Some((Ask,s)) => EurexSnapshot(av.ts, Side[Bid](av.bid, Vector()), Side[Ask](av.ask, s))
      }
    }

    })
  }

}