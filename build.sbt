name := "paypay-challenge"

version := "1.0"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2"
libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.9" % "test"
libraryDependencies += "joda-time" % "joda-time" % "2.10.2"
libraryDependencies += "org.joda" % "joda-convert" % "2.2.1"

mainClass in (Compile, run) := Some("paypay.challenge.SessionAnalyzer")
