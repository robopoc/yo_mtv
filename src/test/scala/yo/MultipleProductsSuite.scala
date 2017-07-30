/**
  * Created by robo on 17/7/17.
  */
package yo


import java.sql.Timestamp
import java.time.{ZoneId, ZonedDateTime}

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Dataset, Encoder, Encoders}
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen._
import org.scalacheck.Prop._
import org.scalacheck.{Prop, _}
import org.scalatest.junit.JUnitSuite
import org.scalatest.prop.{Checkers, PropertyChecks}
import org.scalacheck.Arbitrary._
import org.scalacheck.Prop._
import org.scalacheck.Test.Parameters
import org.scalatest.{Matchers, PropSpec}
import yo.sparkSession.implicits._
import yo.MultiSnapsFunctions._
//import yo.SnapsFunctions._

final class MultipleProductsSuite extends PropSpec with PropertyChecks with Matchers {

  val MID_TICK = 10000L
  val MAX_DEV = 1000L

  lazy val tsGen: Gen[Timestamp] = {
    var offset_nanos : Long = 0L
    for {
      i: Long <- Gen.choose(1L,10000000L)
    } yield {
      offset_nanos = offset_nanos + i
      Timestamp.valueOf(MY_EPOCH.plusNanos(offset_nanos).toLocalDateTime)
    }
  }
  implicit lazy val tsArb: Arbitrary[Timestamp] = Arbitrary(tsGen)

  property("continous time generator") {
    var prev_ts = Timestamp.valueOf(MY_EPOCH.plusNanos(1).toLocalDateTime)
    forAll {
      (ts: Timestamp) => {
        val before = ts.after(prev_ts)
        prev_ts = ts
        before should be(true)
      }
    }
  }

  lazy val rsGen : Gen[EurexSnapshot] = {
    var midTick : Long = 10000L
    for {
      received: Timestamp <- tsGen
      dev : Long <- Gen.choose[Long](-1L,1L)
    } yield {
      midTick = Math.max(midTick + dev, 10000L - 1000L)
      EurexSnapshot(received,
        Side[Bid](Vector(PriceVolume(midTick - 1L,10L)),Vector()),
        Side[Ask](Vector(PriceVolume(midTick + 1L,10L)),Vector()))
    }
  }
  implicit lazy val rsDt: Arbitrary[Snapshot] = Arbitrary(rsGen)

//  property("snapshot generation") {
//    forAll { (snap: Snapshot) =>
//      snap.asks.quotes should have size 1
//      snap.asks.trades shouldBe empty
//
//      snap.bids.quotes should have size 1
//      snap.bids.trades shouldBe empty
//
//      snap.asks.quotes(0).tickPrice should be > snap.bids.quotes(0).tickPrice
//
//      snap.received.after(Timestamp.valueOf(MY_EPOCH.toLocalDateTime)) should equal (true)
//    }
//  }

  lazy val rsListGen : Gen[List[EurexSnapshot]] = listOf[EurexSnapshot](rsGen)
  implicit lazy val arbRsList: Arbitrary[List[EurexSnapshot]] = Arbitrary(rsListGen)

//  property("snapshot generator") {
//    forAll{ (es: List[EurexSnapshot]) =>
//      es should not be empty
//    }
//  }

  lazy val rsDSGen : Gen[Snaps] = {
    for {
      rsList : List[EurexSnapshot] <- rsListGen
    } yield rsList.toDS()
  }
  implicit lazy val arbDS: Arbitrary[Snaps] = Arbitrary(rsDSGen)


//
//
//  property("multi prod cont time 2") {
//    forAll {
//      (ds1: EurexSnaps, ds2: EurexSnaps) => {
//          val prodMap: Map[String, EurexSnaps] = Map(("FDAX", ds1), ("FESX", ds2))
//          val ms = new MultipleProducts().combine_products(prodMap)
//          ms.filter(m => m.received != latest_snap(m.productMap).received).collect() shouldBe empty
//      }
//    }
//  }

 // implicit val sss: Encoder[yo.Snapshot] = ExpressionEncoder()
 // implicit val enc: Encoder[Map[String, yo.Snapshot]] = ExpressionEncoder()


  property("x y z") {
    forAll {
      (ds1: Snaps, ds2: Snaps) => {
          val init = sparkSession.emptyDataset[MultiSnapshot]
          val ms = List(("FDAX", ds1),("FESX", ds2)).foldLeft[MultiSnaps](init)(_ addProduct _)

          ms.filter(m => m.received != m.latest().received).collect() shouldBe empty
          //ms.filter(m => m.received == new Timestamp(0L)).collect() shouldBe empty

      }
    }
  }


//  property("snaps generator") {
//    forAll {
//      (es : EurexSnaps) => {
//        es.columns should contain ("received")
//        es.columns should contain ("bids")
//        es.columns should contain ("asks")
//      }
//    }
//  }



}


