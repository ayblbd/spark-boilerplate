import sbt.Keys.libraryDependencies

name := "spark-boilerplate"

version := "0.1"

val sparkVersion = "2.4.7"

// https://scalacenter.github.io/scalafix/docs/users/installation.html
inThisBuild(
  List(
    scalaVersion := "2.12.12",
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
  "com.typesafe" % "config" % "1.4.1",
  "me.vican.jorge" %% "dijon" % "0.4.0" % "test",
  "org.scalatest" %% "scalatest-flatspec" % "3.2.2" % "test",
  "com.github.mrpowers" %% "spark-fast-tests" % "0.21.3" % "test"
)
