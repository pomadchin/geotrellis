import Dependencies._

name := "geotrellis-cassandra"

def exclusionRule(organization: String, name: String = "", artifact: String = "", configurations: Vector[String] = Vector()) = 
  ExclusionRule(organization, name, artifact, configurations)

libraryDependencies ++= Seq(
  "com.datastax.cassandra" % "cassandra-driver-core" % Version.cassandra
    excludeAll (
      exclusionRule("org.jboss.netty"), exclusionRule("io.netty"),
      exclusionRule("org.slf4j"), exclusionRule("io.spray"), exclusionRule("com.typesafe.akka")
    ) exclude("org.apache.hadoop", "hadoop-client"),
  sparkCore % "provided",
  spire,
  scalatest % "test")

fork in Test := false
parallelExecution in Test := false

initialCommands in console :=
  """
  import geotrellis.raster._
  import geotrellis.vector._
  import geotrellis.proj4._
  import geotrellis.spark._
  import geotrellis.spark.util._
  import geotrellis.spark.tiling._
  import geotrellis.spark.io.cassandra._
  """
