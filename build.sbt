
name := "GraphX"

version := "1.0"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.2.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.2.0",
  "org.apache.spark" % "spark-graphx_2.11" % "2.2.0",
  "org.mongodb.spark" % "mongo-spark-connector_2.11" % "2.2.0"
)
        