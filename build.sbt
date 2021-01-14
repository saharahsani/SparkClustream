name := "SparkClu-V1"

version := "0.1"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.2"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.5.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.2"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.5.2"
libraryDependencies  ++= Seq(
  // Last stable release
  "org.scalanlp" %% "breeze" % "0.12",

  // Native libraries are not included by default. add this if you want them
  // Native libraries greatly improve performance, but increase jar sizes.
  // It also packages various blas implementations, which have licenses that may or may not
  // be compatible with the Apache License. No GPL code, as best I know.
  "org.scalanlp" %% "breeze-natives" % "0.12",

  // The visualization library is distributed separately as well.
  // It depends on LGPL code
  "org.scalanlp" %% "breeze-viz" % "0.12"
)
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.8.2.1"
libraryDependencies += "org.apache.kafka" %% "kafka" % "0.8.2.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.5.2"