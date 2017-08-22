package org.apache.spark.ml.yo.mtv

import org.apache.spark.annotation.Since
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.BinaryAttribute
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCol, HasInputCols, HasOutputCol}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, DoubleType, StructField, StructType}

/**
  * box cut into
  */
final class BoxCut(override val uid: String)
  extends Transformer with HasInputCols with HasOutputCol with DefaultParamsWritable {


  def this() = this(Identifiable.randomUID("boxcut"))

  val thresholds: DoubleArrayParam =
  new DoubleArrayParam(this, "thresholds", "thresholds used to cut")

  /** @group getParam */
  def getThreshold: Array[Double] = $(thresholds)

  /** @group setParam */
  def setThreshold(value: Array[Double]): this.type = set(thresholds, value)

  setDefault(thresholds -> Array())

  val ops: IntArrayParam = new IntArrayParam(this, "ops", "ops used to cut")

  def getOp(): Array[Int] = $(ops)

  def setOp(value: Array[Int]): this.type = set(ops, value)

  setDefault(ops -> Array())


  /** @group setParam */
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  /** @group setParam */
  @Since("1.4.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema, logging = true)
    val tds = if ($(ops).isEmpty) $(thresholds).map(t => (t,true)) else $(thresholds).zip($(ops).map(o => o > 0.5))
    def cutDouble(thresholds: Array[(Double,Boolean)]) =
      udf { (in: Seq[Any]) => if
        (in.zip(thresholds).forall(g => g match {
          case (null, (t: Any, grt: Any)) => false
          case (v: Int, (t: Double, grt: Boolean)) => (grt && (v > t)) || (!grt && (v < t))
          case (v: Double, (t: Double, grt: Boolean)) => (grt && (v > t)) || (!grt && (v < t))
        })) 1.0
        else 0.0
      }

    val metadata = outputSchema($(outputCol)).metadata

    dataset.withColumn($(outputCol), cutDouble(tds)(array($(inputCols).map(c => dataset(c)) :_*)))
  }

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {
    val inputType = $(inputCols).map(s => schema(s).dataType)
    val outputColName = $(outputCol)

    val outCol: StructField = BinaryAttribute.defaultAttr.withName(outputColName).toStructField()
//    inputType match {
//      case DoubleType =>
//
//      case _: VectorUDT =>
//        StructField(outputColName, new VectorUDT)
//      case _ =>
//        throw new IllegalArgumentException(s"Data type $inputType is not supported.")
//    }

    if (schema.fieldNames.contains(outputColName)) {
      throw new IllegalArgumentException(s"Output column $outputColName already exists.")
    }
    StructType(schema.fields :+ outCol)
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): Binarizer = defaultCopy(extra)
}

@Since("1.6.0")
object BoxCut extends DefaultParamsReadable[Binarizer] {

  @Since("1.6.0")
  override def load(path: String): Binarizer = super.load(path)
}
