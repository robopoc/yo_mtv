name := "yo_mtv"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"
// "2.1.1"

// libraryDependencies += "junit" % "junit" % "4.10" % "test"


// https://mvnrepository.com/artifact/com.cloudera.sparkts/sparkts
libraryDependencies += "com.cloudera.sparkts" % "sparkts" % "0.4.0"
//libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.1.1_0.7.2" % "test"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.13.4" % "test"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

// https://mvnrepository.com/artifact/com.databricks/spark-avro_2.11
libraryDependencies += "com.databricks" %% "spark-avro" % "3.2.0"

// https://mvnrepository.com/artifact/com.google.cloud.bigdataoss/gcs-connector
libraryDependencies += "com.google.cloud.bigdataoss" % "gcs-connector" % "1.6.1-hadoop2"



