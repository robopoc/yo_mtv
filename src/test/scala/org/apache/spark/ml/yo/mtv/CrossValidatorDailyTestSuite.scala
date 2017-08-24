package org.apache.spark.ml.yo.mtv

import org.apache.spark.ml.yo.mtv.evaluation.PredictionEvaluator
import org.apache.spark.sql.Dataset
import org.scalacheck._
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}
import org.apache.spark.ml.yo.sparkSession.implicits._
import org.apache.spark.ml.yo.{Tov, Yo}

/**
  * Created by robo on 17/7/17.
  */
final class CrossValidatorDailyTestSuite extends PropSpec with PropertyChecks with Matchers {



  property("test cvd") {
    val cvd = new CrossValidatorDaily().setEvaluator(new PredictionEvaluator())
    forAll {
      (yds: Dataset[Yo]) => {
        val cvm = cvd.fit(yds)
        val df = cvm.transform(yds)
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
