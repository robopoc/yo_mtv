name := "yo_mtv"

version := "1.0"

scalaVersion := "2.11.8"

// libraryDependencies += "junit" % "junit" % "4.10" % "test"


// https://mvnrepository.com/artifact/com.cloudera.sparkts/sparkts
libraryDependencies += "com.cloudera.sparkts" % "sparkts" % "0.4.0"
//libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.1.1_0.7.2" % "test"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.1"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.1.1"

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.13.4" % "test"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

