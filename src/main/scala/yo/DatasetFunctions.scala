package yo

import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.sql.{Dataset, Encoder}
import sparkSession.implicits._

/**
  * Created by robo on 28/7/17.
  */
@InterfaceStability.Stable
class DatasetFunctions[T](private val ds: Dataset[T]) extends Serializable {

  def scanPartition[S](z: S, f: (S,T) => S): Dataset[S] = ???
//    def scanner(i: Iterator[T]): Iterator[S] = {
//      val it = i.scanLeft(z)(f)
//      it.next()
//      it
//    }
//    ds.mapPartitions(scanner)
//  }
}

object DatasetFunctions {
  implicit def addFunctions(ds: MultiSnaps) = new DatasetFunctions(ds)
}
