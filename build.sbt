name := "kaggHousPrice"

scalaVersion := "2.11.12"

val spark_version = "2.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % spark_version,
  "org.apache.spark" %% "spark-sql" % spark_version,
  "org.apache.spark" %% "spark-mllib" % spark_version,
//  "ml.dmlc" % "xgboost4j" % "0.72",
//  "ml.dmlc" % "xgboost4j-spark" % "0.90",
//  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "com.typesafe" % "config" % "1.3.1")
mainClass in assembly := Some("mainclass")
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}