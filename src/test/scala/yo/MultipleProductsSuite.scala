/**
  * Created by robo on 17/7/17.
  */
package yo


import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.time.temporal.{ChronoField, TemporalField}
import java.time.{LocalDateTime, ZoneId, ZoneOffset, ZonedDateTime}

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
import com.databricks.spark.avro._
import com.google.cloud.hadoop.fs.gcs._
import com.google.cloud.hadoop.util._


final class MultipleProductsSuite extends PropSpec with PropertyChecks with Matchers {

  val MID_TICK = 10000L
  val MAX_DEV = 1000L

  lazy val dayGen: Gen[Long] = {
    var prev_day = -1L
    for {
      offset: Long <- Gen.choose(1L,10L)
    } yield {
      prev_day += offset
      prev_day
    }
  }

  lazy val tsGen: Gen[Timestamp] = {
    var offset_nanos : Long = 0L
    for {
      d: Long <- dayGen
      i: Long <- Gen.choose(1L,10000000L)
    } yield {
      offset_nanos = offset_nanos + i
      Timestamp.valueOf(MY_EPOCH.plusDays(d).plusNanos(offset_nanos).toLocalDateTime)
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
    val sdf = new SimpleDateFormat("yyyyMMdd")
    def toDate(ts: Timestamp): Int = {
      Integer.parseInt(sdf.format(new Date(ts.getTime)))
    }
    for {
      received: Timestamp <- tsGen
      dev : Long <- Gen.choose[Long](-1L,1L)
    } yield {
      midTick = Math.max(midTick + dev, 10000L - 1000L)
      EurexSnapshot(received.getTime * 1000000L + received.getNanos,
        toDate(received),
        Side[Bid](Vector(PriceVolume(midTick - 1L,10L)),Vector()),
        Side[Ask](Vector(PriceVolume(midTick + 1L,10L)),Vector()))
    }
  }
  implicit lazy val rsDt: Arbitrary[Snapshot] = Arbitrary(rsGen)

  property("snapshot generation") {
    val ts = Timestamp.valueOf(MY_EPOCH.toLocalDateTime)
      forAll { (snap: Snapshot) =>
      snap.asks.quotes should have size 1
      snap.asks.trades shouldBe empty

      snap.bids.quotes should have size 1
      snap.bids.trades shouldBe empty

      snap.asks.quotes(0).tickPrice should be > snap.bids.quotes(0).tickPrice

      snap.received should be > ts.getTime * 1000000L + ts.getNanos
    }
  }

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
    } yield {
      val ds = rsList.toDS()
      rsList match {
        case Nil => ds
        case _ => ds.repartition(rsList.groupBy(es => es.ssd).size, ds("ssd"))
      }
    }
  }
  implicit lazy val arbDS: Arbitrary[Snaps] = Arbitrary(rsDSGen)

  property("Snaps partitions") {
    forAll {
      (s: Snaps) => {
        s.count() match {
          case 0 => s.rdd.getNumPartitions shouldBe 1
          case _ => s.rdd.getNumPartitions shouldBe (s.groupBy(s("ssd")).count().count())
        }
        // partitions need to obey ssd
        s.foreachPartition(ite => {
          val ssd = if (ite.hasNext) ite.next().ssd else 0
          if (!ite.isEmpty && ite.forall(es => es.ssd == ssd)) {
            throw new RuntimeException("yo")
          }
        })
      }
    }
  }


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


  property("test multisnaps add product") {
    var prev_ts = 0L
    forAll {
      (ds1: Snaps, ds2: Snaps) => {
        val ms = createMultiSnaps(List(("FDAX", ds1),("FESX", ds2)))

        ms.products should have size 2

        val numDays = Math.max(1,ms.select("ssd").distinct().count())


        ms.rdd.getNumPartitions shouldBe (numDays)

        //ms.filter(m => m.received != m.latest().received).collect() shouldBe empty

        def bef(tb: ((Long,Boolean),(Long,Boolean))) = {
          (tb._2._1,(tb._2._1 > tb._1._1) && tb._1._2)
        }
        //ms.map(m => (m.received, true)).reduce(bef(_,_))._2 should equal (true)
      }
    }
  }

  property("test avro file conversion") {
    val ds = sparkSession.read.avro("/Users/robo/data/avro_test.avro").as[Avros]
    println(ds.rdd.getNumPartitions)

    //ds.map(av => LocalDateTime.ofEpochSecond(av.ts/1000000000, (av.ts % 1000000000).toInt, ZoneOffset.of("Z")))

    ds.write.partitionBy("ts").avro("/Users/robo/data/part/")
    println(ds.rdd.getNumPartitions)


   // ds.map(r => Math.max(r.bid.length,r.ask.length)).reduce(Math.max(_,_))


//    fdf.show()
//    val ffdf = fdf.filter(av => av.tick != null)
//    ffdf.show()
//
//    val oink = ffdf.select("tick.Source").distinct().collect()
  }

  lazy val msGen : Gen[MultiSnaps] = {
    for {
      snapsList : List[Snaps] <- Gen.listOf[Snaps](rsDSGen)
    } yield createMultiSnaps(snapsList.zipWithIndex.map(s => ("p" + s._2, s._1)))
  }
  implicit lazy val arbMS: Arbitrary[MultiSnaps] = Arbitrary(msGen)

  property("test multisnaps fill") {
    forAll {
      (ms: MultiSnaps) => {
        if (ms == null || ms.count() == 0) {
          0 shouldBe (0)
        }
        else {
          val ms_filled = ms.fill()

          ms_filled.count() shouldBe (ms.count())
          ms_filled.rdd.getNumPartitions shouldBe (ms.rdd.getNumPartitions)
          val prod_sizes = ms_filled.map(m => m.products.size).cache()
          prod_sizes.distinct().count() should equal(1)
          prod_sizes.filter(f => f != ms_filled.products.size).collect shouldBe empty

          ms.isTradedProduct(ms.products.head._1)
        }
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


