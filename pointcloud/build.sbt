import Dependencies._

name := "geotrellis-pointcloud"

val circeVersion = "0.7.0"

libraryDependencies ++= Seq(
  sparkCore % "provided",
  pdalScala,
  scalatest % "test")

fork in Test := true
parallelExecution in Test := false

javaOptions += s"-Djava.library.path=${Environment.ldLibraryPath}"
