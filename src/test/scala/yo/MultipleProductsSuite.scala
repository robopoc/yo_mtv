/**
  * Created by robo on 17/7/17.
  */
package yo


import java.time.{ZoneId, ZonedDateTime}

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

final class MultipleProductsSuite extends PropSpec with PropertyChecks with Matchers {

  val MID_TICK = 10000L
  val MAX_DEV = 1000L

  lazy val dtContGen : Gen[ZonedDateTime] = {
    var offset_nanos : Long = 0L
    for {
      i : Long <- Gen.choose(1L,10000000L)
    } yield {
      offset_nanos = offset_nanos + i
      MY_EPOCH.plusNanos(offset_nanos)
    }
  }
  implicit lazy val arbDt: Arbitrary[ZonedDateTime] = Arbitrary(dtContGen)

  property("continous time generator") {
    var prev_zdt = MY_EPOCH
    forAll {
      (zdt: ZonedDateTime) => {
        val before = zdt.isAfter(prev_zdt)
        prev_zdt = zdt
        before should be(true)
     }
    }
  }

  lazy val rsGen : Gen[EurexSnapshot] = {
    var midTick : Long = 10000L
    for {
      received: ZonedDateTime <- dtContGen
      dev : Long <- Gen.choose[Long](-1L,1L)
    } yield {
      midTick = Math.max(midTick + dev, 10000L - 1000L)
      EurexSnapshot(received,
        Side(Bid(), List(PriceVolume(midTick - 1L,10L)),List()),
        Side(Ask(),List(PriceVolume(midTick + 1L,10L)),List()))
    }
  }
  implicit lazy val rsDt: Arbitrary[Snapshot] = Arbitrary(rsGen)

  property("snapshot generation") {
    forAll { (snap: Snapshot) =>
      snap.asks.quotes should have size 1
      snap.asks.trades shouldBe empty
      snap.asks.side should matchPattern { case Ask() => }

      snap.bids.quotes should have size 1
      snap.bids.trades shouldBe empty
      snap.bids.side should matchPattern { case Bid() => }

      snap.asks.quotes(0).tickPrice should be > snap.bids.quotes(0).tickPrice

      snap.received.isAfter(MY_EPOCH) should equal (true)
    }
  }

  lazy val rsListGen : Gen[List[EurexSnapshot]] = listOf[EurexSnapshot](rsGen)
  implicit lazy val arbRsList: Arbitrary[List[EurexSnapshot]] = Arbitrary(rsListGen)

  property("snapshot generator") {
    forAll{ (es: List[EurexSnapshot]) =>
      es should not be empty
    }
  }

  lazy val rsDSGen : Gen[EurexSnaps] = {
    for {
      rsList : List[EurexSnapshot] <- rsListGen
    } yield rsList.toDS()
  }
  implicit lazy val arbDS: Arbitrary[EurexSnaps] = Arbitrary(rsDSGen)

  property("snaps generator") {
    forAll {
      (es : EurexSnaps) => {
        es.count() > 0
        println(es.columns)
        es.columns should contain ("received")
      }
    }
  }

  property("multi prod cont time") {
    def latest_snap = (l : Iterable[Snapshot]) => l.reduce((z1,z2) => if (z1.received.isAfter(z2.received)) z1 else z2)
    forAll {
      (ds1: EurexSnaps, ds2: EurexSnaps) => {
        if (ds1.count() > 0 && ds2.count > 0) {
        val prodMap: Map[Product, EurexSnaps] = Map(FDAX() -> ds1, FSTX() -> ds2)
        val ms = new MultipleProducts().combine_products(prodMap)
        //ms("received") should be (sorted[ZonedDateTime])
        // ms("received") should equal (ms.map(m => last_snap(m.productMap.values).received))
        ms.filter(m => m.received != latest_snap(m.productMap.values).received).collect() shouldBe empty
      }
      }
    }
  }

}


