package org.apache.spark.ml.yo

import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.sql.Dataset

/**
  * Created by robo on 28/7/17.
  */
@InterfaceStability.Stable
class DatasetFunctions[T](private val ds: Dataset[T]) extends Serializable {

  def scanPartition[S](z: S, f: (S,T) => S): Dataset[S] = ??? //{
//    import sparkSession.implicits._
//    ds.mapPartitions(i => {
//      val it = i.scanLeft(z)(f)
//      it.next()
//      it
//    })
//  }
}

object DatasetFunctions {
  implicit def addFunctions(ds: MultiSnaps) = new DatasetFunctions(ds)

}
