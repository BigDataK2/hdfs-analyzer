name := "hdfs-analyzer"

fork := true

parallelExecution in Test := false

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.3.1" % "provided",
  "org.apache.spark" % "spark-sql_2.10" % "1.3.1" % "provided",
  "org.apache.spark" % "spark-hive_2.10" % "1.3.1" % "provided",
  "org.slf4j" % "slf4j-log4j12" % "1.7.13" % "provided",
  "me.lessis" %% "courier" % "0.1.3",
  "com.github.nscala-time" %% "nscala-time" % "2.10.0",
  "org.scalatest" % "scalatest_2.10" % "2.2.1" % "test"
)

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
  case PathList("org", "apache", xs@_*) => MergeStrategy.first
  case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
