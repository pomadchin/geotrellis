/*
 * Copyright (c) 2018 Azavea.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import Dependencies._
import GTBenchmarkPlugin.Keys._
import sbt._
import sbt.Keys._
import sbtassembly.AssemblyPlugin.autoImport.{MergeStrategy, assemblyMergeStrategy}
import sbtassembly.AssemblyKeys._
import sbtassembly.{PathList, ShadeRule}
import com.typesafe.tools.mima.plugin.MimaKeys._
import sbtprotoc.ProtocPlugin.autoImport.PB

object Settings {
  object Repositories {
    val locationtechReleases  = "locationtech-releases" at "https://repo.locationtech.org/content/repositories/releases/"
    val locationtechSnapshots = "locationtech-snapshots" at "https://repo.locationtech.org/content/repositories/snapshots/"
    val boundlessgeo          = "boundlessgeo" at "http://repo.boundlessgeo.com/main/"
    val boundlessgeoRelease   = "boundless" at "https://repo.boundlessgeo.com/release"
    val geosolutions          = "geosolutions" at "http://maven.geo-solutions.it/"
    val osgeo                 = "osgeo" at "http://download.osgeo.org/webdav/geotools/"
    val ivy2Local             = Resolver.file("local", file(Path.userHome.absolutePath + "/.ivy2/local"))(Resolver.ivyStylePatterns)
    val mavenLocal            = Resolver.mavenLocal
    val local                 = Seq(ivy2Local, mavenLocal)
    val azaveaBintray         = Resolver.bintrayRepo("azavea", "geotrellis")
  }

  lazy val noForkInTests = Seq(
    fork in Test := false,
    parallelExecution in Test := false
  )

  lazy val accumulo = Seq(
    name := "geotrellis-accumulo",
    libraryDependencies ++= Seq(
      accumuloCore
        exclude("org.jboss.netty", "netty")
        exclude("org.apache.hadoop", "hadoop-client"),
      spire,
      scalatest % Test,
      hadoopClient % Provided
    ),
    initialCommands in console :=
      """
      import geotrellis.proj4._
      import geotrellis.vector._
      import geotrellis.raster._
      import geotrellis.layer._
      import geotrellis.store.accumulo._
      """
  ) ++ noForkInTests

  lazy val `accumulo-spark` = Seq(
    name := "geotrellis-accumulo-spark",
    libraryDependencies ++= Seq(
      accumuloCore
        exclude("org.jboss.netty", "netty")
        exclude("org.apache.hadoop", "hadoop-client"),
      sparkCore % Provided,
      spire,
      sparkSQL % Test,
      scalatest % Test
    ),
    initialCommands in console :=
      """
      import geotrellis.raster._
      import geotrellis.vector._
      import geotrellis.proj4._
      import geotrellis.layer._
      import geotrellis.store.accumulo._
      import geotrellis.spark._
      import geotrellis.spark.store.accumulo._
      """
  ) ++ noForkInTests

  lazy val bench = Seq(
    libraryDependencies += sl4jnop,
    jmhIterations := Some(5),
    jmhTimeUnit := None, // Each benchmark should determing the appropriate time unit.
    jmhExtraOptions := Some("-jvmArgsAppend -Xmx8G")
    //jmhExtraOptions := Some("-jvmArgsAppend -Xmx8G -prof jmh.extras.JFR")
    //jmhExtraOptions := Some("-jvmArgsAppend -prof geotrellis.bench.GeotrellisFlightRecordingProfiler")
  )

  lazy val cassandra = Seq(
    name := "geotrellis-cassandra",
    libraryDependencies ++= Seq(
      cassandraDriverCore
        excludeAll(
        ExclusionRule("org.jboss.netty"), ExclusionRule("io.netty"),
        ExclusionRule("org.slf4j"), ExclusionRule("com.typesafe.akka")
      ) exclude("org.apache.hadoop", "hadoop-client"),
      spire,
      scalatest % Test
    ),
    initialCommands in console :=
      """
      import geotrellis.proj4._
      import geotrellis.vector._
      import geotrellis.layer._
      import geotrellis.raster._
      import geotrellis.store.util._
      import geotrellis.store.cassandra._
      """
  ) ++ noForkInTests

  lazy val `cassandra-spark` = Seq(
    name := "geotrellis-cassandra-spark",
    libraryDependencies ++= Seq(
      cassandraDriverCore
        excludeAll(
        ExclusionRule("org.jboss.netty"), ExclusionRule("io.netty"),
        ExclusionRule("org.slf4j"), ExclusionRule("com.typesafe.akka")
      ) exclude("org.apache.hadoop", "hadoop-client"),
      sparkCore % Provided,
      spire,
      sparkSQL % Test,
      scalatest % Test
    ),
    initialCommands in console :=
      """
      import geotrellis.raster._
      import geotrellis.vector._
      import geotrellis.proj4._
      import geotrellis.layer._
      import geotrellis.store.cassandra._
      import geotrellis.spark._
      import geotrellis.spark.util._
      import geotrellis.spark.store.cassandra._
      """
  ) ++ noForkInTests


  lazy val `doc-examples` = Seq(
    name := "geotrellis-doc-examples",
    publish / skip := true,
    libraryDependencies ++= Seq(
      sparkCore,
      logging,
      scalatest % Test,
      sparkSQL % Test
    )
  )

  lazy val geomesa = Seq(
    name := "geotrellis-geomesa",
    libraryDependencies ++= Seq(
      geomesaAccumuloJobs,
      geomesaAccumuloDatastore,
      geomesaUtils,
      sparkCore % Provided,
      spire,
      sparkSQL % Test,
      scalatest % Test
    ),
    resolvers ++= Seq(
      Repositories.boundlessgeo,
      Repositories.locationtechReleases
    ),
    initialCommands in console :=
      """
      import geotrellis.raster._
      import geotrellis.vector._
      import geotrellis.proj4._
      import geotrellis.layer._
      import geotrellis.spark._
      import geotrellis.spark.util._
      import geotrellis.spark.io.geomesa._
      """
  ) ++ noForkInTests

  lazy val geotools = Seq(
    name := "geotrellis-geotools",
    libraryDependencies ++= Seq(
      geotoolsCoverage,
      geotoolsHsql,
      geotoolsMain,
      geotoolsReferencing,
      jts,
      spire,
      geotoolsGeoTiff % Test,
      geotoolsShapefile % Test,
      scalatest % Test,
      // This is one finicky dependency. Being explicit in hopes it will stop hurting Travis.
      jaiCore % Test from "http://download.osgeo.org/webdav/geotools/javax/media/jai_core/1.1.3/jai_core-1.1.3.jar"
    ),
    externalResolvers ++= Seq(
      Repositories.boundlessgeo
    ),
    initialCommands in console :=
      """
      import geotrellis.geotools._
      import geotrellis.raster._
      import geotrellis.vector._
      import org.locationtech.jts.{geom => jts}
      import org.geotools.coverage.grid._
      import org.geotools.coverage.grid.io._
      import org.geotools.gce.geotiff._
      """,
    testOptions in Test += Tests.Setup { () => Unzip.geoTiffTestFiles() }
  ) ++ noForkInTests

  lazy val geowave = Seq(
    name := "geotrellis-geowave",
    libraryDependencies ++= Seq(
      accumuloCore
        exclude("org.jboss.netty", "netty")
        exclude("org.apache.hadoop", "hadoop-client"),
      geowaveRaster
        excludeAll(ExclusionRule(organization = "org.mortbay.jetty"),
        ExclusionRule(organization = "javax.servlet")),
      geowaveVector
        excludeAll(ExclusionRule(organization = "org.mortbay.jetty"),
        ExclusionRule(organization = "javax.servlet")),
      geowaveStore
        excludeAll(ExclusionRule(organization = "org.mortbay.jetty"),
        ExclusionRule(organization = "javax.servlet")),
      geowaveGeotime
        excludeAll(ExclusionRule(organization = "org.mortbay.jetty"),
        ExclusionRule(organization = "javax.servlet")),
      geowaveAccumulo
        excludeAll(ExclusionRule(organization = "org.mortbay.jetty"),
        ExclusionRule(organization = "javax.servlet")),
      hadoopClient % Provided
        excludeAll(ExclusionRule(organization = "org.mortbay.jetty"),
        ExclusionRule(organization = "javax.servlet")),
      geotoolsCoverage % Provided
        excludeAll(ExclusionRule(organization = "org.mortbay.jetty"),
        ExclusionRule(organization = "javax.servlet")),
      geotoolsHsql % Provided
        excludeAll(ExclusionRule(organization = "org.mortbay.jetty"),
        ExclusionRule(organization = "javax.servlet")),
      geotoolsMain % Provided
        excludeAll(ExclusionRule(organization = "org.mortbay.jetty"),
        ExclusionRule(organization = "javax.servlet")),
      geotoolsReferencing % Provided
        excludeAll(ExclusionRule(organization = "org.mortbay.jetty"),
        ExclusionRule(organization = "javax.servlet")),
      scalaArm,
      kryoSerializers exclude("com.esotericsoftware", "kryo"),
      kryoShaded,
      sparkCore % Provided,
      spire,
      sparkSQL % Test,
      scalatest % Test
    ),
    resolvers ++= Seq(
      Repositories.boundlessgeoRelease,
      Repositories.geosolutions,
      Repositories.osgeo
    ),
    assemblyMergeStrategy in assembly := {
      case "reference.conf" => MergeStrategy.concat
      case "application.conf" => MergeStrategy.concat
      case PathList("META-INF", xs@_*) =>
        xs match {
          case ("MANIFEST.MF" :: Nil) => MergeStrategy.discard
          // Concatenate everything in the services directory to keep GeoTools happy.
          case ("services" :: _ :: Nil) =>
            MergeStrategy.concat
          // Concatenate these to keep JAI happy.
          case ("javax.media.jai.registryFile.jai" :: Nil) | ("registryFile.jai" :: Nil) | ("registryFile.jaiext" :: Nil) =>
            MergeStrategy.concat
          case (name :: Nil) => {
            // Must exclude META-INF/*.([RD]SA|SF) to avoid "Invalid signature file digest for Manifest main attributes" exception.
            if (name.endsWith(".RSA") || name.endsWith(".DSA") || name.endsWith(".SF"))
              MergeStrategy.discard
            else
              MergeStrategy.first
          }
          case _ => MergeStrategy.first
        }
      case _ => MergeStrategy.first
    },
    initialCommands in console :=
      """
      """
  ) ++ noForkInTests

  lazy val hbase = Seq(
    name := "geotrellis-hbase",
    libraryDependencies ++= Seq(
      hbaseCommon exclude("javax.servlet", "servlet-api"),
      hbaseClient exclude("javax.servlet", "servlet-api"),
      hbaseMapReduce exclude("javax.servlet", "servlet-api"),
      hbaseServer exclude("org.mortbay.jetty", "servlet-api-2.5"),
      hbaseHadoopCompact exclude("javax.servlet", "servlet-api"),
      hbaseHadoop2Compact exclude("javax.servlet", "servlet-api"),
      hbaseMetrics exclude("javax.servlet", "servlet-api"),
      hbaseMetricsApi exclude("javax.servlet", "servlet-api"),
      hbaseZooKeeper exclude("javax.servlet", "servlet-api"),
      jacksonCoreAsl,
      spire,
      sparkSQL % Test,
      scalatest % Test
    ),
    /** https://github.com/lucidworks/spark-solr/issues/179 */
    dependencyOverrides ++= {
      val deps = Seq(
        "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7",
        "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7",
        "com.fasterxml.jackson.core" % "jackson-annotations" % "2.6.7"
      )
      CrossVersion.partialVersion(scalaVersion.value) match {
        // if Scala 2.12+ is used
        case Some((2, scalaMajor)) if scalaMajor >= 12 => deps
        case _ => deps :+ "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.7"
      }
    },
    initialCommands in console :=
      """
      import geotrellis.raster._
      import geotrellis.vector._
      import geotrellis.proj4._
      import geotrellis.layer._
      import geotrellis.store.util._
      import geotrellis.store.hbase._
      """
  ) ++ noForkInTests

  lazy val `hbase-spark` = Seq(
    name := "geotrellis-hbase-spark",
    libraryDependencies ++= Seq(
      hbaseCommon exclude("javax.servlet", "servlet-api"),
      hbaseClient exclude("javax.servlet", "servlet-api"),
      hbaseMapReduce exclude("javax.servlet", "servlet-api"),
      hbaseServer exclude("org.mortbay.jetty", "servlet-api-2.5"),
      hbaseHadoopCompact exclude("javax.servlet", "servlet-api"),
      hbaseHadoop2Compact exclude("javax.servlet", "servlet-api"),
      hbaseMetrics exclude("javax.servlet", "servlet-api"),
      hbaseMetricsApi exclude("javax.servlet", "servlet-api"),
      hbaseZooKeeper exclude("javax.servlet", "servlet-api"),
      jacksonCoreAsl,
      sparkCore % Provided,
      spire,
      sparkSQL % Test,
      scalatest % Test
    ),
    /** https://github.com/lucidworks/spark-solr/issues/179 */
    dependencyOverrides ++= {
      val deps = Seq(
        "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7",
        "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7",
        "com.fasterxml.jackson.core" % "jackson-annotations" % "2.6.7"
      )
      CrossVersion.partialVersion(scalaVersion.value) match {
        // if Scala 2.12+ is used
        case Some((2, scalaMajor)) if scalaMajor >= 12 => deps
        case _ => deps :+ "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.7"
      }
    },
    initialCommands in console :=
      """
      import geotrellis.raster._
      import geotrellis.vector._
      import geotrellis.proj4._
      import geotrellis.layer._
      import geotrellis.store.util._
      import geotrellis.spark._
      import geotrellis.spark.store.hbase._
      import geotrellis.store.hbase._
      """
  ) ++ noForkInTests

  lazy val macros = Seq(
    name := "geotrellis-macros",
    sourceGenerators in Compile += (sourceManaged in Compile).map(Boilerplate.genMacro).taskValue,
    libraryDependencies ++= Seq(
      spireMacro,
      "org.scala-lang" % "scala-reflect" % scalaVersion.value
    ),
    resolvers += Resolver.sonatypeRepo("snapshots")
  )

  lazy val proj4 = Seq(
    name := "geotrellis-proj4",
    libraryDependencies ++= Seq(
      proj4j,
      openCSV,
      parserCombinators,
      scalatest % Test,
      scalacheck % Test,
      scaffeine
    ),
    // https://github.com/sbt/sbt/issues/4609
    fork in Test := true
  )

  lazy val raster = Seq(
    name := "geotrellis-raster",
    libraryDependencies ++= Seq(
      pureconfig,
      jts,
      catsCore,
      spire,
      squants,
      monocleCore,
      monocleMacro,
      scalaXml,
      scalaURI,
      scalatest % Test,
      scalacheck % Test
    ),
    mimaPreviousArtifacts := Set(
      "org.locationtech.geotrellis" %% "geotrellis-raster" % Version.previousVersion
    ),
    sourceGenerators in Compile += (sourceManaged in Compile).map(Boilerplate.genRaster).taskValue,
    initialCommands in console :=
      """
      import geotrellis.raster._
      import geotrellis.raster.resample._
      import geotrellis.vector._
      import geotrellis.raster.io.geotiff._
      import geotrellis.raster.render._
      """,
    testOptions in Test += Tests.Setup { () =>
      val testArchive = "raster/data/geotiff-test-files.zip"
      val testDirPath = "raster/data/geotiff-test-files"
      if (!(new File(testDirPath)).exists) {
        Unzip(testArchive, "raster/data")
      }
    }
  )

  lazy val `raster-testkit` = Seq(
    name := "geotrellis-raster-testkit",
    libraryDependencies += scalatest
  )

  lazy val s3 = Seq(
    name := "geotrellis-s3",
    libraryDependencies ++= Seq(
      awsSdkS3,
      spire,
      scaffeine,
      scalatest % Test
    ),
    dependencyOverrides ++= {
      val deps = Seq(
        "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7",
        "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7",
        "com.fasterxml.jackson.core" % "jackson-annotations" % "2.6.7"
      )
      CrossVersion.partialVersion(scalaVersion.value) match {
        // if Scala 2.12+ is used
        case Some((2, scalaMajor)) if scalaMajor >= 12 => deps
        case _ => deps :+ "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.7"
      }
    },
    mimaPreviousArtifacts := Set(
      "org.locationtech.geotrellis" %% "geotrellis-s3" % Version.previousVersion
    ),
    initialCommands in console :=
      """
      import geotrellis.raster._
      import geotrellis.vector._
      import geotrellis.proj4._
      import geotrellis.layer._
      import geotrellis.store.s3._
      """
  ) ++ noForkInTests

  lazy val `s3-spark` = Seq(
    name := "geotrellis-s3-spark",
    libraryDependencies ++= Seq(
      sparkCore % Provided,
      awsSdkS3,
      spire,
      scaffeine,
      sparkSQL % Test,
      scalatest % Test
    ),
    dependencyOverrides ++= {
      val deps = Seq(
        "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7",
        "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7",
        "com.fasterxml.jackson.core" % "jackson-annotations" % "2.6.7"
      )
      CrossVersion.partialVersion(scalaVersion.value) match {
        // if Scala 2.12+ is used
        case Some((2, scalaMajor)) if scalaMajor >= 12 => deps
        case _ => deps :+ "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.7"
      }
    },
    mimaPreviousArtifacts := Set(
      "org.locationtech.geotrellis" %% "geotrellis-s3" % Version.previousVersion
    ),
    initialCommands in console :=
      """
      import geotrellis.raster._
      import geotrellis.vector._
      import geotrellis.proj4._
      import geotrellis.layer._
      import geotrellis.store.s3._
      import geotrellis.store.util._
      import geotrellis.spark._
      import geotrellis.spark.store.s3._
      """
  ) ++ noForkInTests

  lazy val shapefile = Seq(
    name := "geotrellis-shapefile",
    libraryDependencies ++= Seq(
      geotoolsShapefile,
      // This is one finicky dependency. Being explicit in hopes it will stop hurting Travis.
      jaiCore from "http://download.osgeo.org/webdav/geotools/javax/media/jai_core/1.1.3/jai_core-1.1.3.jar"
    ),
    resolvers += Repositories.osgeo,
    fork in Test := false
  )

  lazy val spark = Seq(
    name := "geotrellis-spark",
    libraryDependencies ++= Seq(
      sparkCore % Provided,
      hadoopClient % Provided,
      uzaygezenCore,
      scalaj,
      avro,
      spire,
      monocleCore, monocleMacro,
      chronoscala,
      catsCore,
      catsEffect,
      fs2Core,
      fs2Io,
      sparkSQL % Test,
      scalatest % Test,
      logging,
      scaffeine
    ),
    mimaPreviousArtifacts := Set(
      "org.locationtech.geotrellis" %% "geotrellis-spark" % Version.previousVersion
    ),
    testOptions in Test += Tests.Argument("-oD"),
    initialCommands in console :=
      """
      import geotrellis.raster._
      import geotrellis.vector._
      import geotrellis.proj4._
      import geotrellis.layer._
      import geotrellis.spark._
      import geotrellis.spark.util._
      """
  ) ++ noForkInTests

  lazy val `spark-pipeline` = Seq(
    name := "geotrellis-spark-pipeline",
    libraryDependencies ++= Seq(
      circeCore,
      circeGeneric,
      circeGenericExtras,
      circeParser,
      sparkCore % Provided,
      sparkSQL % Test,
      scalatest % Test
    ),
    test in assembly := {},
    assemblyShadeRules in assembly := {
      val shadePackage = "com.azavea.shaded.demo"
      Seq(
        ShadeRule.rename("com.google.common.**" -> s"$shadePackage.google.common.@1")
          .inLibrary("com.azavea.geotrellis" %% "geotrellis-cassandra" % version.value).inAll,
        ShadeRule.rename("io.netty.**" -> s"$shadePackage.io.netty.@1")
          .inLibrary("com.azavea.geotrellis" %% "geotrellis-hbase" % version.value).inAll,
        ShadeRule.rename("com.fasterxml.jackson.**" -> s"$shadePackage.com.fasterxml.jackson.@1")
          .inLibrary(jsonSchemaValidator).inAll,
        ShadeRule.rename("shapeless.**" -> s"$shadePackage.shapeless.@1").inAll
      )
    },
    assemblyMergeStrategy in assembly := {
      case s if s.startsWith("META-INF/services") => MergeStrategy.concat
      case "reference.conf" | "application.conf"  => MergeStrategy.concat
      case "META-INF/MANIFEST.MF" | "META-INF\\MANIFEST.MF" => MergeStrategy.discard
      case "META-INF/ECLIPSEF.RSA" | "META-INF/ECLIPSEF.SF" => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  )

  lazy val `spark-testkit` = Seq(
    name := "geotrellis-spark-testkit",
    libraryDependencies ++= Seq(
      sparkCore % Provided,
      sparkSQL % Provided,
      hadoopClient % Provided,
      scalatest,
      chronoscala
    )
  )

  lazy val util = Seq(
    name := "geotrellis-util",
    libraryDependencies ++= Seq(
      logging,
      pureconfig,
      spire,
      scalatest % Test
    )
  )

  lazy val vector = Seq(
    name := "geotrellis-vector",
    libraryDependencies ++= Seq(
      jts,
      pureconfig,
      circeCore,
      circeGeneric,
      circeGenericExtras,
      circeParser,
      apacheMath,
      spire,
      scalatest % Test,
      scalacheck % Test
    )
  )

  lazy val `vector-testkit` = Seq(
    name := "geotrellis-vector-testkit",
    libraryDependencies += scalatest
  )

  lazy val vectortile = Seq(
    name := "geotrellis-vectortile",
    libraryDependencies ++= Seq(
      scalatest % Test,
      scalapbRuntime % "protobuf"
    ),
    PB.protoSources in Compile := Seq(file("vectortile/data")),
    PB.targets in Compile := Seq(
      scalapb.gen(flatPackage = true, grpc = false) -> (sourceManaged in Compile).value
    )
  )

  lazy val layer = Seq(
    name := "geotrellis-layer",
    libraryDependencies ++= Seq(
      hadoopClient % Provided,
      apacheIO,
      avro,
      spire,
      monocleCore,
      monocleMacro,
      chronoscala,
      catsEffect,
      spire,
      fs2Core,
      fs2Io,
      logging,
      scaffeine,
      uzaygezenCore,
      pureconfig,
      scalactic,
      scalatest % Test
    ),
    initialCommands in console :=
      """
      import geotrellis.raster._
      import geotrellis.vector._
      import geotrellis.proj4._
      import geotrellis.layer._
      """
  )

  lazy val store = Seq(
    name := "geotrellis-store",
    libraryDependencies ++= Seq(
      hadoopClient % Provided,
      apacheIO,
      avro,
      spire,
      monocleCore,
      monocleMacro,
      chronoscala,
      catsEffect,
      spire,
      fs2Core,
      fs2Io,
      logging,
      scaffeine,
      uzaygezenCore,
      pureconfig,
      scalaXml,
      scalatest % Test
    )
  )

  lazy val gdal = Seq(
    name := "geotrellis-gdal",
    libraryDependencies ++= Seq(
      gdalWarp,
      scalatest % Test,
      gdalBindings % Test
    ),
    resolvers += Repositories.azaveaBintray,
    Test / fork := true,
    Test / parallelExecution := false,
    Test / testOptions += Tests.Argument("-oDF"),
    javaOptions ++= Seq("-Djava.library.path=/usr/local/lib")
  )

  lazy val `gdal-spark` = Seq(
    name := "geotrellis-gdal-spark",
    libraryDependencies ++= Seq(
      gdalWarp,
      sparkCore % Provided,
      sparkSQL % Test,
      scalatest % Test
    ),
    // caused by the AWS SDK v2
    dependencyOverrides ++= {
      val deps = Seq(
        jacksonCore,
        jacksonDatabind,
        jacksonAnnotations
      )
      CrossVersion.partialVersion(scalaVersion.value) match {
        // if Scala 2.12+ is used
        case Some((2, scalaMajor)) if scalaMajor >= 12 => deps
        case _ => deps :+ jacksonModuleScala
      }
    },
    resolvers += Repositories.azaveaBintray,
    Test / fork := true,
    Test / parallelExecution := false,
    Test / testOptions += Tests.Argument("-oDF"),
    javaOptions ++= Seq("-Djava.library.path=/usr/local/lib")
  )
}
