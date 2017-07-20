name := "yo_mtv"

version := "1.0"

scalaVersion := "2.12.2"

// libraryDependencies += "junit" % "junit" % "4.10" % "test"


// https://mvnrepository.com/artifact/com.cloudera.sparkts/sparkts
libraryDependencies += "com.cloudera.sparkts" % "sparkts" % "0.4.0"
//libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.1.1_0.7.2" % "test"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.1"
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.1"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.1.1"

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.13.4" % "test"

