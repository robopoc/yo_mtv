package yo

/**
  * Created by robo on 17/7/17.
  */

import java.time.{ZoneId, ZonedDateTime}

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._
import org.apache.spark.sql.{Dataset, SparkSession}

class MultipleProductsSuite extends Properties("Multi Product Support") {
  val sparkSession = SparkSession.builder.appName("multi product test suite").master("local[*]").getOrCreate()
  import sparkSession.implicits._

  lazy val dtContGen : Gen[ZonedDateTime] = {
    var offset_nanos : Long = 0L
    for {
      i : Long <- Gen.choose(1L,10000000L)
    } yield {
      offset_nanos = offset_nanos + i
      ZonedDateTime.of(2017,7,1,0,0,0,100, ZoneId.systemDefault()).plusNanos(offset_nanos)
    }
  }
  implicit lazy val arbDt: Arbitrary[ZonedDateTime] = Arbitrary(dtContGen)

  property("continous time") = {
    var czdt = ZonedDateTime.of(2017,7,1,0,0,0,100, ZoneId.systemDefault())
    forAll { (zdt: ZonedDateTime) =>
      val before = zdt.isBefore(czdt)
      czdt = zdt
      before
    }
  }

  lazy val rsGen : Gen[RetailState] = {
    var midTick : Long = 10000L
    for {
      received: ZonedDateTime <- dtContGen
      dev : Long <- Gen.choose[Long](-1L,1L)
    } yield {
      midTick = Math.max(midTick + dev, 10000L - 1000L)
      RetailState(received,
        Side(Bid(), List(PriceVolume(midTick - 1L,10L)),List()),
        Side(Ask(),List(PriceVolume(midTick + 1L,10L)),List()))
    }
  }

  lazy val rsListGen : Gen[List[RetailState]] = listOf[RetailState](rsGen)

  lazy val rsDSGen : Gen[Dataset[RetailState]] = {
    for {
      rsList : List[RetailState] <- rsListGen
    } yield rsList.toDS()
  }
  implicit lazy val arbDS: Arbitrary[Dataset[RetailState]] = Arbitrary(rsDSGen)

  property("multi prod cont time") = forAll { (ds1: Dataset[RetailState], ds2: Dataset[RetailState]) =>
    MultipleProducts()
  }
}


