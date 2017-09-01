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

package geotrellis.raster.reproject

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.testkit._
import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.vector.testkit._

import org.scalatest._
import spire.syntax.cfor._

class ReprojectRasterExtentSpec extends FunSpec
    with TileBuilders
    with GeoTiffTestUtils
    with RasterMatchers {
  describe("ReprojectRasterExtent") {

    def formatExtent(name: String, re: RasterExtent) = {
      val e = re.extent
      val es = f"Extent(${e.xmin}%1.5f, ${e.ymin}%1.5f, ${e.xmax}%1.5f, ${e.ymax}%1.5f)"
      f"$name: $es ${re.cellwidth}%1.5f, ${re.cellheight}%1.5f  (${re.cols} x ${re.rows})"
    }

    it("should (approximately) match a GDAL for EPSG:4326 to EPSG:3857") {
      val sourceGt = SinglebandGeoTiff.compressed(geoTiffPath("reproject/nlcd_tile_wsg84.tif"))
      val sourceRaster = sourceGt.raster

      val rea @ RasterExtent(actualExtent, actualCellWidth, actualCellHeight, actualCols, actualRows) =
        ReprojectRasterExtent(sourceRaster.rasterExtent, sourceGt.crs, WebMercator)

      val ree @ RasterExtent(expectedExtent, expectedCellWidth, expectedCellHeight, expectedCols, expectedRows) =
        SinglebandGeoTiff(geoTiffPath("reproject/nlcd_tile_webmercator-nearestneighbor.tif")).raster.rasterExtent

      // println(formatExtent("GTA", rea))
      // println(formatExtent("EXP", ree))

      actualExtent.toPolygon should matchGeom (expectedExtent.toPolygon, 1.0)
      actualCols should be (expectedCols +- 1)
      actualRows should be (expectedRows +- 1)
    }

    it("should be in approximation to GDAL EPSG:32618 to EPSG:3857") {
      val extent = Extent(394800.000, 4567140.000, 410160.000, 4582500.000)
      val (cols, rows) = (512, 512)
      val rasterExtent = RasterExtent(extent, cols, rows)

      val expectedExtent = Extent(-8489029.279, 5049106.840, -8468324.230, 5069891.832)
      val (expectedCols, expectedRows) = (518, 520)
      val ree @ RasterExtent(_, expectedCellWidth, expectedCellHeight, _, _) = RasterExtent(expectedExtent, expectedCols, expectedRows)

      val src = CRS.fromEpsgCode(32618)
      val dest = CRS.fromEpsgCode(3857)

      val rea @ RasterExtent(actualExtent, actualCellWidth, actualCellHeight, actualCols, actualRows) = ReprojectRasterExtent(rasterExtent, src, dest)

      // println(formatExtent("GTA", rea))
      // println(formatExtent("EXP", ree))

      actualExtent.toPolygon should matchGeom (expectedExtent.toPolygon, 1.0)
      actualCols should be (expectedCols +- 1)
      actualRows should be (expectedRows +- 1)
    }

    it("should be in approximation to GDAL EPSG:32618 to EPSG:4326") {
      val extent = Extent(394800.000, 4567140.000, 410160.000, 4582500.000)
      val (cols, rows) = (512, 512)
      val rasterExtent = RasterExtent(extent, cols, rows)

      val expectedExtent = Extent(-76.2582475,  41.2488530, -76.0722134,  41.3890157)
      val (expectedCols, expectedRows) = (584, 440)
      val ree = RasterExtent(expectedExtent, expectedCols, expectedRows)

      val src = CRS.fromEpsgCode(32618)
      val dest = CRS.fromEpsgCode(4326)

      val rea @ RasterExtent(actualExtent, _, _, actualCols, actualRows) = ReprojectRasterExtent(rasterExtent, src, dest)

      // println(formatExtent("GTA", rea))
      // println(formatExtent("EXP", ree))

      actualExtent.toPolygon should matchGeom (expectedExtent.toPolygon, 1.0)
      actualCols should be (expectedCols +- 1)
      actualRows should be (expectedRows +- 1)
    }
  }
}
