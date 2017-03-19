name := "spark-meta"

version := "0.1.0"

scalaVersion := "2.11.8"

sparkVersion := "2.1.0"

sparkComponents += "sql"

libraryDependencies ++= Seq(
  "com.holdenkarau" % "spark-testing-base_2.11" % "2.0.1_0.4.7"
)

sparkComponents ++= Seq("sql","hive")

parallelExecution in Test := false