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

package geotrellis.spark.summary.polygonal

import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.testkit.testfiles._
import geotrellis.raster.summary.polygonal._
import geotrellis.spark.testkit._

import geotrellis.raster._
import geotrellis.vector._

import org.scalatest.FunSpec

class SumDoubleSpec extends FunSpec with TestEnvironment with TestFiles {

  describe("Sum Double Zonal Summary Operation") {
    val ones = AllOnesTestFile
    val multi = ones.withContext { _.mapValues { tile => MultibandTile(tile, tile, tile) }}

    val tileLayout = ones.metadata.tileLayout
    val count = (ones.count * tileLayout.tileCols * tileLayout.tileRows).toInt
    val totalExtent = ones.metadata.extent

    val xd = totalExtent.xmax - totalExtent.xmin
    val yd = totalExtent.ymax - totalExtent.ymin

    val quarterExtent = Extent(
      totalExtent.xmin,
      totalExtent.ymin,
      totalExtent.xmin + xd / 2,
      totalExtent.ymin + yd / 2
    )

    val p1 = Point(totalExtent.xmin + xd / 2, totalExtent.ymax)
    val p2 = Point(totalExtent.xmax, totalExtent.ymin + yd / 2)
    val p3 = Point(totalExtent.xmin + xd / 2, totalExtent.ymin)
    val p4 = Point(totalExtent.xmin, totalExtent.ymin + yd / 2)

    val diamondPoly = Polygon(Line(Array(p1, p2, p3, p4, p1)))

    val polyWithHole = {
      val exterior = Line(Array(p1, p2, p3, p4, p1))

      val pi1 = Point(totalExtent.xmin + xd / 2, totalExtent.ymax - yd / 4)
      val pi2 = Point(totalExtent.xmax - xd / 4, totalExtent.ymin + yd / 2)
      val pi3 = Point(totalExtent.xmin + xd / 2, totalExtent.ymin + yd / 4)
      val pi4 = Point(totalExtent.xmin + xd / 4, totalExtent.ymin + yd / 2)

      val interior = Line(Array(pi1, pi2, pi3, pi4, pi1))

      Polygon(exterior, interior)
    }

    it("should get correct double sum over whole raster extent") {
      ones.polygonalSumDouble(totalExtent.toPolygon) should be(count)
    }

    it("should get correct double sum over whole raster extent for MultibandTileRDD") {
      multi.polygonalSumDouble(totalExtent.toPolygon) map { _  should be(count) }
    }

    it("should get correct double sum over a quarter of the extent") {
      val result = ones.polygonalSumDouble(quarterExtent.toPolygon)
      val expected = ones.stitch.tile.polygonalSumDouble(totalExtent, quarterExtent.toPolygon)

      result should be (expected)
    }

    it("should get correct double sum over a quarter of the extent for MultibandTileRDD") {
      val result = multi.polygonalSumDouble(quarterExtent.toPolygon)
      val expected = multi.stitch.tile.polygonalSumDouble(totalExtent, quarterExtent.toPolygon)

      result zip expected map { case (res, exp) =>
        res should be (exp)
      }
    }

    it("should get correct double sum over half of the extent in diamond shape") {
      val result = ones.polygonalSumDouble(diamondPoly)
      val expected = ones.stitch.tile.polygonalSumDouble(totalExtent, diamondPoly)

      result should be (expected)
    }

    it("should get correct double sum over half of the extent in diamond shape for MultibandTileRDD") {
      val result = multi.polygonalSumDouble(diamondPoly)
      val expected = multi.stitch.tile.polygonalSumDouble(totalExtent, diamondPoly)

      result zip expected map { case (res, exp) =>
        res should be (exp)
      }
    }

    it("should get correct double sum over polygon with hole") {
      val result = ones.polygonalSumDouble(polyWithHole)
      val expected = ones.stitch.tile.polygonalSumDouble(totalExtent, polyWithHole)

      result should be (expected)
    }

    it("should get correct double sum over polygon with hole for MultibandTileRDD") {
      val result = multi.polygonalSumDouble(polyWithHole)
      val expected = multi.stitch.tile.polygonalSumDouble(totalExtent, polyWithHole)

      result zip expected map { case (res, exp) =>
        res should be (exp)
      }
    }
  }

  describe("Sum Double Zonal Summary Operation (collections api)") {
    val ones = AllOnesTestFile.toCollection
    val multi = AllOnesTestFile.withContext { _.mapValues { tile => MultibandTile(tile, tile, tile) }}

    val tileLayout = ones.metadata.tileLayout
    val count = ones.length * tileLayout.tileCols * tileLayout.tileRows
    val totalExtent = ones.metadata.extent

    val xd = totalExtent.xmax - totalExtent.xmin
    val yd = totalExtent.ymax - totalExtent.ymin

    val quarterExtent = Extent(
      totalExtent.xmin,
      totalExtent.ymin,
      totalExtent.xmin + xd / 2,
      totalExtent.ymin + yd / 2
    )

    val p1 = Point(totalExtent.xmin + xd / 2, totalExtent.ymax)
    val p2 = Point(totalExtent.xmax, totalExtent.ymin + yd / 2)
    val p3 = Point(totalExtent.xmin + xd / 2, totalExtent.ymin)
    val p4 = Point(totalExtent.xmin, totalExtent.ymin + yd / 2)

    val diamondPoly = Polygon(Line(Array(p1, p2, p3, p4, p1)))

    val polyWithHole = {
      val exterior = Line(Array(p1, p2, p3, p4, p1))

      val pi1 = Point(totalExtent.xmin + xd / 2, totalExtent.ymax - yd / 4)
      val pi2 = Point(totalExtent.xmax - xd / 4, totalExtent.ymin + yd / 2)
      val pi3 = Point(totalExtent.xmin + xd / 2, totalExtent.ymin + yd / 4)
      val pi4 = Point(totalExtent.xmin + xd / 4, totalExtent.ymin + yd / 2)

      val interior = Line(Array(pi1, pi2, pi3, pi4, pi1))

      Polygon(exterior, interior)
    }

    it("should get correct double sum over whole raster extent") {
      ones.polygonalSumDouble(totalExtent.toPolygon) should be(count)
    }

    it("should get correct double sum over whole raster extent for MultibandTiles") {
      multi.polygonalSumDouble(totalExtent.toPolygon) map { _  should be(count) }
    }

    it("should get correct double sum over a quarter of the extent") {
      val result = ones.polygonalSumDouble(quarterExtent.toPolygon)
      val expected = ones.stitch.tile.polygonalSumDouble(totalExtent, quarterExtent.toPolygon)

      result should be (expected)
    }

    it("should get correct double sum over a quarter of the extent for MultibandTiles") {
      val result = multi.polygonalSumDouble(quarterExtent.toPolygon)
      val expected = multi.stitch.tile.polygonalSumDouble(totalExtent, quarterExtent.toPolygon)

      result zip expected map { case (res, exp) =>
        res should be (exp)
      }
    }

    it("should get correct double sum over half of the extent in diamond shape") {
      val result = ones.polygonalSumDouble(diamondPoly)
      val expected = ones.stitch.tile.polygonalSumDouble(totalExtent, diamondPoly)

      result should be (expected)
    }

    it("should get correct double sum over half of the extent in diamond shape for MultibandTiles") {
      val result = multi.polygonalSumDouble(diamondPoly)
      val expected = multi.stitch.tile.polygonalSumDouble(totalExtent, diamondPoly)

      result zip expected map { case (res, exp) =>
        res should be (exp)
      }
    }

    it("should get correct double sum over polygon with hole") {
      val result = ones.polygonalSumDouble(polyWithHole)
      val expected = ones.stitch.tile.polygonalSumDouble(totalExtent, polyWithHole)

      result should be (expected)
    }

    it("should get correct double sum over polygon with hole for MultibandTiles") {
      val result = multi.polygonalSumDouble(polyWithHole)
      val expected = multi.stitch.tile.polygonalSumDouble(totalExtent, polyWithHole)

      result zip expected map { case (res, exp) =>
        res should be (exp)
      }
    }
  }
}
