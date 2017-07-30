import java.time.{ZoneId, ZonedDateTime}

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by robo on 21/7/17.
  */
package object yo {
  type Snaps = Dataset[EurexSnapshot]

  //type EurexSnaps = Snaps[EurexSnapshot]
  type MultiSnaps = Dataset[MultiSnapshot]

  val sparkSession = SparkSession.builder.appName("yo").master("local[*]").getOrCreate()
  val MY_EPOCH = ZonedDateTime.of(2017,7,1,0,0,0,100, ZoneId.systemDefault())
}
