name := "spark-boilerplate"

version := "0.1"

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

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7"

// https://mvnrepository.com/artifact/org.apache.spark/spark-hive
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.7"

// https://mvnrepository.com/artifact/com.typesafe/config
libraryDependencies += "com.typesafe" % "config" % "1.4.1"

libraryDependencies += "me.vican.jorge" %% "dijon" % "0.4.0" % "test"

libraryDependencies += "org.scalatest" %% "scalatest-flatspec" % "3.2.2" % "test"

libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "0.21.3" % "test"
