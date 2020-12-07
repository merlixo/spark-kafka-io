organization := "chug.spark"
name := "spark-kafka-io"
version := "0.0.1"

scalaVersion := "2.12.8"
crossScalaVersions := Seq("2.11.12", "2.12.8")
val sparkVersion = "2.4.3"

scalacOptions ++= Seq(
  "-unchecked",
  "-deprecation",
  "-feature",
  "-Ywarn-unused-import",
  "-Ywarn-unused"
)

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % Provided
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion

parallelExecution in Test := false