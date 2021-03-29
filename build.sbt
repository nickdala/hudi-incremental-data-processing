name := "hudi-incremental-data-processing"

version := "0.1"

scalaVersion := "2.12.13"

libraryDependencies ++= {
  val sparkVersion = "3.1.1"
  val hudiVersion = "0.7.0"

  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion  % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion  % "provided",
    "org.apache.hudi" %% "hudi-spark3" % hudiVersion % "provided"
  )
}