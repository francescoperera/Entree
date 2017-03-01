name := "Entree"

version := "1.0"

scalaVersion := "2.12.1"


// Circe
val circeVersion = "0.7.0-M2"
libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser",
  "io.circe" %% "circe-java8"
).map(_ % circeVersion)

//Http
libraryDependencies += "org.scalaj" % "scalaj-http_2.12" % "2.3.0"

//Logging
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.22"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7"

// AWS
libraryDependencies += "com.github.seratch" %% "awscala" % "0.5.9"
