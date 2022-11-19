import sbt.Keys.libraryDependencies

name := "spark-boilerplate"

version := "0.1"

val sparkVersion: String = "2.4.8"
val hbaseVersion: String = "2.3.0"

// https://scalacenter.github.io/scalafix/docs/users/installation.html
inThisBuild(
  List(
    scalaVersion := "2.11.12",
    semanticdbEnabled := true, // enable SemanticDB
    semanticdbVersion := scalafixSemanticdb.revision // use Scalafix compatible version
  )
)

scalacOptions ++= List(
  "-Ywarn-unused"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "com.typesafe" % "config" % "1.4.2",
  "org.apache.hbase" % "hbase-client" % hbaseVersion,
  "org.apache.hbase" % "hbase-server" % hbaseVersion,
  "org.apache.hbase" % "hbase-mapreduce" % hbaseVersion,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.4",
  "org.scalatest" %% "scalatest-flatspec" % "3.2.14" % "test",
  "com.github.mrpowers" %% "spark-fast-tests" % "0.23.0" % "test"
)
