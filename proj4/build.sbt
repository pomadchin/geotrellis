import Dependencies._

name := "geotrellis-proj4"

libraryDependencies ++= Seq(
  openCSV,
  parserCombinators,
  scalatest   % "test",
  scalacheck  % "test")

headerLicense := Some(HeaderLicense.ALv2("2016", "Azavea"))