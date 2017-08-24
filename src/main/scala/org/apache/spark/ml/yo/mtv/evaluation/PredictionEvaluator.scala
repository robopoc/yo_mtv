package org.apache.spark.ml.yo.mtv.evaluation

import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.param.shared.HasPredictionCol
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.ml.yo.sparkSession.implicits._

trait HasValuationCol extends Params {

  /**
    * Param for valuation column name.
    * @group param
    */
  final val valuationCol: Param[String] = new Param[String](this, "valuationCol", "valuation column name")

  setDefault(valuationCol, "prediction")

  /** @group getParam */
  final def getValuationCol: String = $(valuationCol)
}

class PredictionEvaluator extends Evaluator with HasPredictionCol with HasValuationCol {
  override def copy(extra: ParamMap) = defaultCopy(extra)

  override val uid = Identifiable.randomUID("prediction evaluation")

  override def evaluate(dataset: Dataset[_]): Double = {
    def predVal(it: Iterator[Row]) = {
      Seq((1,it.map(r => r.getAs[Double](0) * r.getAs[Double](1)).sum)).iterator
    }

    val res = dataset.select(dataset(getPredictionCol),dataset(getValuationCol))
      .mapPartitions(predVal).reduce((a,b) => (a._1 + b._1, a._2 + b._2))
    res._2 / res._1
  }
}
