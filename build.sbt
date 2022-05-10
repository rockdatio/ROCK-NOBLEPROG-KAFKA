name := "Kstream-Master-Class"


version := "1.0"

scalaVersion := "2.12.13"

val kafkaVersion = "2.8.1"
//val kafkaVersion = "0.10.0.0"
val scala_Version = "2.12.13"

libraryDependencies += "com.google.code.gson" % "gson" % "2.8.5"

libraryDependencies ++= Seq(
  //"org.scala-lang" % "scala-library" % scala_Version,
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion,
  "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion,
  "org.asynchttpclient" % "async-http-client" % "2.4.5",
  "com.mashape.unirest" % "unirest-java" % "1.4.9",
  "log4j" % "log4j" % "1.2.14"
)


