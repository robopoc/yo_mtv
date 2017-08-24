package org.apache.spark.ml.yo.mtv

import com.github.fommil.netlib.F2jBLAS
import org.apache.spark.annotation.Since
import org.apache.spark.ml.{Model, PredictionModel, Predictor}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCols, HasOutputCol}
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable, Instrumentation}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel}
import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.sql.{DataFrame, Dataset}
import scala.reflect.ClassTag

/**
  * box cut into
  */
final class CrossValidatorDaily(override val uid: String) extends CrossValidator {
  private val f2jBLAS = new F2jBLAS

  def split[T: ClassTag](data: RDD[T], numFolds: Int, seed: Long): Array[(RDD[T],RDD[T])] = {
    val numPart = data.getNumPartitions
    val foldSize = Math.max(1,numPart/numFolds)
    def ohboy(activeFold: Int, foldSize: Int)(compl: Boolean)(pos: Int): Boolean = {
      val fold = pos / foldSize
      if (!compl) fold == activeFold else fold != activeFold
    }

    def pairPrunedRDD(activeFold: Int): (RDD[T], RDD[T]) = {
      val oha = ohboy(activeFold, foldSize)(_)
      val test = PartitionPruningRDD.create(data, oha(false))
      val valid = PartitionPruningRDD.create(data, oha(true))
      (test,valid)
    }

    (1 to numFolds).map(af => pairPrunedRDD(af)).toArray
  }

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): CrossValidatorModel = {
    val schema = dataset.schema
    transformSchema(schema, logging = true)
    val sparkSession = dataset.sparkSession
    val est = $(estimator)
    val eval = $(evaluator)
    val epm = $(estimatorParamMaps)
    val numModels = epm.length
    val metrics = new Array[Double](epm.length)

    val instr = Instrumentation.create(this, dataset)
    instr.logParams(numFolds, seed)
    logTuningParams(instr)

    val splits = split(dataset.toDF.rdd, $(numFolds), $(seed))
    splits.zipWithIndex.foreach { case ((training, validation), splitIndex) =>
      val trainingDataset = sparkSession.createDataFrame(training, schema).cache()
      val validationDataset = sparkSession.createDataFrame(validation, schema).cache()
      // multi-model training
      logDebug(s"Train split $splitIndex with multiple sets of parameters.")
      val models = est.fit(trainingDataset, epm).asInstanceOf[Seq[Model[_]]]
      trainingDataset.unpersist()
      var i = 0
      while (i < numModels) {
        // TODO: duplicate evaluator to take extra params from input
        val metric = eval.evaluate(models(i).transform(validationDataset, epm(i)))
        logDebug(s"Got metric $metric for model trained with ${epm(i)}.")
        metrics(i) += metric
        i += 1
      }
      validationDataset.unpersist()
    }
    f2jBLAS.dscal(numModels, 1.0 / $(numFolds), metrics, 1)
    logInfo(s"Average cross-validation metrics: ${metrics.toSeq}")
    val (bestMetric, bestIndex) =
      if (eval.isLargerBetter) metrics.zipWithIndex.maxBy(_._1)
      else metrics.zipWithIndex.minBy(_._1)
    logInfo(s"Best set of parameters:\n${epm(bestIndex)}")
    logInfo(s"Best cross-validation metric: $bestMetric.")
    val bestModel = est.fit(dataset, epm(bestIndex)).asInstanceOf[Model[_]]
    instr.logSuccess(bestModel)
    copyValues(new CrossValidatorModel(uid, bestModel, metrics).setParent(this))
  }
}

