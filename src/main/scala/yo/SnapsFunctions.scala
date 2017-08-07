package yo

import java.security.InvalidParameterException
import java.sql.{Date, Timestamp}
import java.time.temporal.ChronoField
import java.time.{Instant, LocalDate, ZoneId}

import com.databricks.spark.avro._
import org.apache.spark.annotation.InterfaceStability
import sparkSession.implicits._
import MultiSnapsFunctions._
/**
  * Created by robo on 28/7/17.
  */
//@InterfaceStability.Stable
class SnapsFunctions(private val ds: Snaps) extends Serializable {
//  def toMultiSnaps(product: Prod): MultiSnaps = ds.count() match {
//    case (0) => throw new InvalidParameterException("Won't create MultiSnaps from empty Snaps")
//    case _ => ds.map(s => MultiSnapshot(s.received, Map((product, s))))
//

  def rep(): Unit = {
    ds.repartition(ds("ssd"))
  }

  def toMultiSnaps(product: String): MultiSnaps = {
    val ms: MultiSnaps = ds.map(s => MultiSnapshot(s.received, s.ssd, Vector((product, Some(s)))))
    ms
  }
}

//
object SnapsFunctions {
  implicit def adddFunctions(ds: Snaps) = new SnapsFunctions(ds)

  def fromAvro(product: (String,Map[Date,String]), days: Iterable[Date]): Snaps = {
    val ds = sparkSession.read.avro("/Users/robo/data/avro_test.avro").as[Avros]
    val ds_prod = ds.filter(av => av.feedcode.contains("FUT_NK225_"))

    def trades(tick: PVS, bid: Vector[(Long,Double)], ask: Vector[(Long,Double)]): Option[(_ <: BidAsk, Vector[PriceVolume])] = {
      val side: Option[BidAsk] = tick match {
        case null => None
        case _ => tick.Source match {
          case "TICK_NORMAL" => {
            if (!bid.isEmpty && bid.head._2 == tick.price) Some(Bid())
            else if (!ask.isEmpty && ask.head._2 == tick.price) Some(Ask())
            else None
          }
          case _ => None
        }
      }
      side match {
        case None => None
        case Some(s) => Some(s,Vector(PriceVolume(Math.round(tick.price), Math.round(tick.volume))))
      }
    }

    def ttopv(v: Vector[(Long,Double)]): Vector[PriceVolume] = v.map(t => PriceVolume(Math.round(t._2), t._1))

    ds_prod.map(av => {
      val tr = trades(av.tick, av.bid, av.ask)
      val date = Instant.ofEpochMilli(av.ts/1000000L).get(ChronoField.EPOCH_DAY)
      tr match {
        case Some((Bid(),s)) => EurexSnapshot(av.ts, date, Side[Bid](ttopv(av.bid), s), Side[Ask](ttopv(av.ask), Vector()))
        case Some((Ask(),s)) => EurexSnapshot(av.ts, date, Side[Bid](ttopv(av.bid), Vector()), Side[Ask](ttopv(av.ask), s))
        case _ => EurexSnapshot(av.ts, date, Side[Bid](ttopv(av.bid), Vector()), Side[Ask](ttopv(av.ask), Vector()))
      }
    })
  }

}