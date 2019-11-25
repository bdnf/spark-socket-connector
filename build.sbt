name := "streaming"

version := "0.1"

scalaVersion := "2.12.8"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7"

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.apache.spark" %% "spark-core" % "2.4.3",
  "org.apache.spark" %% "spark-streaming" % "2.4.0",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.0",

  "org.saegesser" %% "scalawebsocket" % "0.2.1",
  "net.liftweb" %% "lift-json" % "3.3.0"
)

