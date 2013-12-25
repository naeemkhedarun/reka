import play.Project._

name := """KafkaTest"""

version := "1.0-SNAPSHOT"

libraryDependencies ++= Seq(
  "org.webjars" %% "webjars-play" % "2.2.0",
  "org.webjars" % "bootstrap" % "2.3.1",
  "org.apache.kafka" % "kafka_2.10" % "0.8.0" excludeAll(ExclusionRule(organization = "com.sun.jdmk"),
                                                         ExclusionRule(organization = "com.sun.jmx")),
  "org.mockito" % "mockito-core" % "1.9.5"
)

playScalaSettings
