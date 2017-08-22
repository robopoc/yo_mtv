package org.apache.spark.ml.yo.mtv

import org.scalatest.{Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks
import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.time.temporal.{ChronoField, TemporalField}
import java.time.{LocalDateTime, ZoneId, ZoneOffset, ZonedDateTime}

import breeze.linalg.sum
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
import yo.SnapsFunctions._
import com.databricks.spark.avro._
import com.google.cloud.hadoop.fs.gcs._
import com.google.cloud.hadoop.util._
import org.apache.spark.{HashPartitioner, RangePartitioner}
import org.apache.spark.sql.catalyst.plans.physical.RangePartitioning
import yo.{MultiSnaps, MultiSnapshot, Tov, Yo}

/**
  * Created by robo on 17/7/17.
  */
final class BoxCutTestSuite extends PropSpec with PropertyChecks with Matchers {

  lazy val yoGen: Gen[Yo] = for {
    s: Int <- Gen.choose[Int](0,1)
  } yield Yo(s)

  lazy val yoDsGen: Gen[Dataset[Yo]] = for {
    y: List[Yo] <- Gen.listOfN(10000, yoGen)
  } yield y.toDS()

  implicit lazy val yoDsArb: Arbitrary[Dataset[Yo]] = Arbitrary(yoDsGen)

  property("test box cut single column") {
    val bc = new BoxCut().setInputCols(Array("siz")).setOutputCol("siz_trigger").setThreshold(Array(.5))
    forAll {
      (yds: Dataset[Yo]) => {
        val df = bc.transform(yds)
        df.count shouldBe (10000)
        df.columns should contain("siz_trigger")
        val c = df.where($"siz_trigger" > 0.5).count().toInt
        val d = df.count().toInt
        val xx: Int = (2 * c - d)
        xx should equal (0 +- 1000)
      }
    }
  }

  lazy val toGen: Gen[Tov] = for {
    s: Int <- Gen.choose[Int](0,1)
    t: Int <- Gen.choose[Int](0,1)
  } yield Tov(s,t)

  lazy val toDsGen: Gen[Dataset[Tov]] = for {
    y: List[Tov] <- Gen.listOfN(10000, toGen)
  } yield y.toDS()

  implicit lazy val toDsArb: Arbitrary[Dataset[Tov]] = Arbitrary(toDsGen)

  property("test box cut two columns") {
    val bc = new BoxCut().setInputCols(Array("siz","tiz")).setOutputCol("siz_trigger").setThreshold(Array(.5,0.5))
    forAll {
      (yds: Dataset[Tov]) => {
        val df = bc.transform(yds)
        df.count shouldBe (10000)
        df.columns should contain("siz_trigger")
        val c = df.where($"siz_trigger" > 0.5).count().toInt
        val d = df.count().toInt
        val xx: Int = (4 * c - d)
        xx should equal (0 +- 1000)
      }
    }
  }
}
