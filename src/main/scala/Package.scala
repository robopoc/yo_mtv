import java.time.{ZoneId, ZonedDateTime}

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders

/**
  * Created by robo on 21/7/17.
  */
package object yo {
  type Snaps[S <: Snapshot] = Dataset[S]
  type EurexSnaps = Snaps[EurexSnapshot]
  type MultiSnaps = Dataset[MultiSnapshot]

  implicit val zdtEncoder: Encoder[ZonedDateTime] = Encoders.kryo[ZonedDateTime]
  implicit def a2: Encoder[java.time.ZonedDateTime] = Encoders.kryo[java.time.ZonedDateTime]

  implicit val eurexSnapshotEncoder: Encoder[EurexSnapshot] = Encoders.kryo[EurexSnapshot]
  implicit def esEncoder: Encoder[EurexSnaps] = Encoders.kryo[EurexSnaps]

  implicit val multiSnapshotEncoder: Encoder[MultiSnapshot] = Encoders.kryo[MultiSnapshot]
  implicit val msEncoder: Encoder[MultiSnaps] = Encoders.kryo[MultiSnaps]


  val sparkSession = SparkSession.builder.appName("yo").master("local[*]").getOrCreate()
  val MY_EPOCH = ZonedDateTime.of(2017,7,1,0,0,0,100, ZoneId.systemDefault())
}
