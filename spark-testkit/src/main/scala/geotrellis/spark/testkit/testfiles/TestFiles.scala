/*
 * Copyright 2016 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.spark.testkit.testfiles

import geotrellis.proj4._
import geotrellis.raster.{GridBounds, TileLayout, FloatConstantNoDataCellType}
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.spark.testkit._

import org.apache.spark._
import jp.ne.opt.chronoscala.Imports._

import java.time.{ZoneOffset, ZonedDateTime}

object TestFiles {
  val ZOOM_LEVEL = 8
  val partitionCount = 4

  def generateSpatial(layerName: String)(implicit sc: SparkContext): TileLayerRDD[SpatialKey] = {
    val md = {
      val cellType = FloatConstantNoDataCellType
      val crs = LatLng
      val tileLayout = TileLayout(8, 8, 3, 4)
      val mapTransform = MapKeyTransform(crs, tileLayout.layoutDimensions)
      val gridBounds = GridBounds(1, 1, 6, 7)
      val extent = mapTransform(gridBounds)
      val keyBounds = KeyBounds(SpatialKey(1,1), SpatialKey(6,7))
      TileLayerMetadata(cellType, LayoutDefinition(crs.worldExtent, tileLayout), extent, crs, keyBounds)
    }

    val gridBounds = md.gridBounds
    val tileLayout = md.tileLayout

    val spatialTestFile = layerName match {
      case "all-ones" => new ConstantSpatialTiles (tileLayout, 1)
      case "all-twos" => new ConstantSpatialTiles (tileLayout, 2)
      case "all-hundreds" => new ConstantSpatialTiles (tileLayout, 100)
      case "increasing" => new IncreasingSpatialTiles (tileLayout, gridBounds)
      case "decreasing" => new DecreasingSpatialTiles (tileLayout, gridBounds)
      case "every-other-undefined" => new EveryOtherSpatialTiles (tileLayout, gridBounds, Double.NaN, 0.0)
      case "every-other-0.99-else-1.01" => new EveryOtherSpatialTiles (tileLayout, gridBounds, 0.99, 1.01)
      case "every-other-1-else-1" => new EveryOtherSpatialTiles (tileLayout, gridBounds, - 1, 1)
      case "mod-10000" => new ModSpatialTiles (tileLayout, gridBounds, 10000)
    }

    val tiles =
      for(
        row <- gridBounds.rowMin to gridBounds.rowMax;
        col <- gridBounds.colMin to gridBounds.colMax
      ) yield {
        val key = SpatialKey(col, row)
        val tile = spatialTestFile(key)
        (key, tile)
      }

    new ContextRDD(sc.parallelize(tiles, partitionCount), md)
  }

  def generateSpaceTime(layerName: String)(implicit sc: SparkContext): TileLayerRDD[SpaceTimeKey] = {
    val times =
      (0 to 4).map(i => ZonedDateTime.of(2010 + i, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)).toArray


    val md = {
      val cellType = FloatConstantNoDataCellType
      val crs = LatLng
      val tileLayout = TileLayout(8, 8, 3, 4)
      val mapTransform = MapKeyTransform(crs, tileLayout.layoutDimensions)
      val gridBounds = GridBounds(1, 1, 6, 7)
      val extent = mapTransform(gridBounds)
      val keyBounds = KeyBounds(SpaceTimeKey(1,1,times.min), SpaceTimeKey(6,7, times.max))
      TileLayerMetadata(cellType, LayoutDefinition(crs.worldExtent, tileLayout), extent, crs, keyBounds)
    }

    val gridBounds = md.gridBounds
    val tileLayout = md.tileLayout

    val spaceTimeTestTiles = layerName match {
      case "spacetime-all-ones" => new ConstantSpaceTimeTestTiles(tileLayout, 1)
      case "spacetime-all-twos" => new ConstantSpaceTimeTestTiles(tileLayout, 2)
      case "spacetime-all-hundreds" => new ConstantSpaceTimeTestTiles(tileLayout, 100)
      case "spacetime-coordinates" => new CoordinateSpaceTimeTestTiles(tileLayout)
    }

    val tiles =
      for(
        row <- gridBounds.rowMin to gridBounds.rowMax;
        col <- gridBounds.colMin to gridBounds.colMax;
        (time, timeIndex) <- times.zipWithIndex
      ) yield {
        val key = SpaceTimeKey(col, row, time)
        val tile = spaceTimeTestTiles(key, timeIndex)
        (key, tile)

      }

    new ContextRDD(sc.parallelize(tiles, partitionCount), md)
  }
}

trait TestFiles { self: TestEnvironment =>
  def spatialTestFile(name: String) = TestFiles.generateSpatial(name)

  def spaceTimeTestFile(name: String) = TestFiles.generateSpaceTime(name)

  lazy val AllOnesTestFile =
    spatialTestFile("all-ones")

  lazy val AllTwosTestFile =
    spatialTestFile("all-twos")

  lazy val AllHundredsTestFile =
    spatialTestFile("all-hundreds")

  lazy val IncreasingTestFile =
    spatialTestFile("increasing")

  lazy val DecreasingTestFile =
    spatialTestFile("decreasing")

  lazy val EveryOtherUndefinedTestFile =
    spatialTestFile("every-other-undefined")

  lazy val EveryOther0Point99Else1Point01TestFile =
    spatialTestFile("every-other-0.99-else-1.01")

  lazy val EveryOther1ElseMinus1TestFile =
    spatialTestFile("every-other-1-else-1")

  lazy val Mod10000TestFile =
    spatialTestFile("mod-10000")

  lazy val AllOnesSpaceTime =
    spaceTimeTestFile("spacetime-all-ones")

  lazy val AllTwosSpaceTime =
    spaceTimeTestFile("spacetime-all-twos")

  lazy val AllHundredsSpaceTime =
    spaceTimeTestFile("spacetime-all-hundreds")

  /** Coordinates are CCC,RRR.TTT where C = column, R = row, T = time (year in 2010 + T).
    * So 34,025,004 would represent col 34, row 25, year 2014
    */
  lazy val CoordinateSpaceTime =
    spaceTimeTestFile("spacetime-coordinates")
}