name := "kafka-spark-hudi-hive-demo"

version := "0.1"

scalaVersion := "2.12.15"

idePackagePrefix := Some("com.whitilied")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.2" % Provided,
  "org.apache.spark" %% "spark-sql" % "3.1.2" % Provided,

  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.1.2",
  "org.apache.spark" %% "spark-avro" % "3.1.2",
  "org.apache.hudi" %% "hudi-spark-bundle" % "0.8.0",
  "org.apache.parquet" % "parquet-hive-bundle" % "1.11.1",

  "joda-time" % "joda-time" % "2.9.9",
)

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs@_*) => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.concat
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
