package org.apache.spark.ml.yo.mtv.target

import org.apache.spark.Aggregator
import org.apache.spark.ml.yo.MultiSnapshot

class Flowers {
  def grow[T <: MultiSnapshot](it: Iterator[T], vall: MultiSnapshot => Double, hl: Long, forward: Boolean = true) = {
    //val ska = if (forward) it.sca

    it.scanLeft((0L,0d))((b,ms) => (ms.received,Math.exp(-(ms.received - b._1) / hl) * b._2 + vall(ms)))
      .filter(f => f._1 != 0L)
  }
}

//class Fu extends Aggregator[] {
//
//}
