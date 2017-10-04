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

package geotrellis.raster.io.geotiff.reader

import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.testkit._

import org.scalatest._

class MultibandGeoTiffReaderSpec extends FunSpec
    with RasterMatchers
    with GeoTiffTestUtils {

  describe("Reading geotiffs with INTERLEAVE=PIXEL") {
    it("Uncompressed, Stripped") {

      val tile =
        reader.GeoTiffReader.readMultiband(geoTiffPath("3bands/int32/3bands-striped-pixel.tif")).tile

      // println("         PIXEL UNCOMPRESSED STRIPPED")
      // println(tile.band(0).asciiDraw)
      // println(tile.band(1).asciiDraw)
      // println(tile.band(2).asciiDraw)

      tile.band(0).foreach { z => z should be (1) }
      tile.band(1).foreach { z => z should be (2) }
      tile.band(2).foreach { z => z should be (3) }
    }

    it("Uncompressed, Tiled") {
      val tile =
        reader.GeoTiffReader.readMultiband(geoTiffPath("3bands/int32/3bands-tiled-pixel.tif")).tile

      // println("         PIXEL UNCOMPRESSED TILED")
      // println(tile.band(0).asciiDraw)
      // println(tile.band(1).asciiDraw)
      // println(tile.band(2).asciiDraw)

      tile.band(0).foreach { (col, row, z) => z should be (1) }
      tile.band(1).foreach { z => z should be (2) }
      tile.band(2).foreach { z => z should be (3) }
    }

    it("COMPRESSION=DEFLATE, Stripped") {
      val tile =
        reader.GeoTiffReader.readMultiband(geoTiffPath("3bands/3bands-deflate.tif")).tile

      // println("         PIXEL COMPRESSED STRIPPED")
      // println(tile.band(0).asciiDraw)
      // println(tile.band(1).asciiDraw)
      // println(tile.band(2).asciiDraw)

      tile.band(0).foreach { z => z should be (1) }
      tile.band(1).foreach { z => z should be (2) }
      tile.band(2).foreach { z => z should be (3) }
    }

    it("COMPRESSION=DEFLATE, Tiled") {
      val tile =
        reader.GeoTiffReader.readMultiband(geoTiffPath("3bands/3bands-tiled-deflate.tif")).tile

      // println("         PIXEL COMPRESSED TILED")
      // println(tile.band(0).asciiDraw)
      // println(tile.band(1).asciiDraw)
      // println(tile.band(2).asciiDraw)

      tile.band(0).foreach { z => z should be (1) }
      tile.band(1).foreach { z => z should be (2) }
      tile.band(2).foreach { z => z should be (3) }
    }
  }

  describe("Reading geotiffs with INTERLEAVE=BANDS") {
    it("Uncompressed, Stripped") {
      val tile =
        reader.GeoTiffReader.readMultiband(geoTiffPath("3bands/int32/3bands-striped-band.tif")).tile


      // println("         PIXEL UNCOMPRESSED STRIPPED")
      // println(tile.band(0).asciiDraw)
      // println(tile.band(1).asciiDraw)
      // println(tile.band(2).asciiDraw)

      tile.band(0).foreach { z => z should be (1) }
      tile.band(1).foreach { z => z should be (2) }
      tile.band(2).foreach { z => z should be (3) }
    }

    it("Uncompressed, Tiled") {
      val tile =
        reader.GeoTiffReader.readMultiband(geoTiffPath("3bands/int32/3bands-tiled-band.tif")).tile

      // println("         BANDS UNCOMPRESSED TILED")
      // println(tile.band(0).asciiDraw)
      // println(tile.band(1).asciiDraw)
      // println(tile.band(2).asciiDraw)

      tile.band(0).foreach { z => z should be (1) }
      tile.band(1).foreach { z => z should be (2) }
      tile.band(2).foreach { z => z should be (3) }
    }

    it("COMPRESSION=DEFLATE, Stripped") {
      val tile =
        reader.GeoTiffReader.readMultiband(geoTiffPath("3bands/3bands-interleave-bands-deflate.tif")).tile

      // println("         BANDS COMPRESSED STRIPPED")
      // println(tile.band(0).asciiDraw)
      // println(tile.band(1).asciiDraw)
      // println(tile.band(2).asciiDraw)

      tile.band(0).foreach { z => z should be (1) }
      tile.band(1).foreach { z => z should be (2) }
      tile.band(2).foreach { z => z should be (3) }
    }

    it("COMPRESSION=DEFLATE, Tiled") {
      val tile =
        reader.GeoTiffReader.readMultiband(geoTiffPath("3bands/3bands-tiled-interleave-bands-deflate.tif")).tile

      // println("         BANDS COMPRESSED TILED")
      // println(tile.band(0).asciiDraw)
      // println(tile.band(1).asciiDraw)
      // println(tile.band(2).asciiDraw)

      tile.band(0).foreach { z => z should be (1) }
      tile.band(1).foreach { z => z should be (2) }
      tile.band(2).foreach { z => z should be (3) }
    }
  }

  describe("reading BIT multiband rasters") {
    def p(s: String, i: String): String =
      geoTiffPath(s"3bands/bit/3bands-${s}-${i}.tif")

    it("should read pixel interleave, striped") {
      val tile: GeoTiffMultibandTile =
        MultibandGeoTiff(
          path = p("striped", "pixel"),
          decompress = false,
          streaming = false
        ).tile.asInstanceOf[GeoTiffMultibandTile]

      // println("         BIT BANDS")
      // println(tile.band(0).asciiDraw)
      // println(tile.band(1).asciiDraw)
      // println(tile.band(2).asciiDraw)

      tile.band(0).foreach { z => z should be (1) }
      tile.band(1).foreach { z => z should be (0) }
      tile.band(2).foreach { z => z should be (1) }
    }

    it("should read pixel interleave, tiled") {
      val tile =
        MultibandGeoTiff(p("tiled", "pixel")).tile

      tile.band(0).foreach { z => z should be (1) }
      tile.band(1).foreach { z => z should be (0) }
      tile.band(2).foreach { z => z should be (1) }
    }

    it("should read band interleave, striped") {
      val tile =
        MultibandGeoTiff(p("striped", "band")).tile

      tile.band(0).foreach { z => z should be (1) }
      tile.band(1).foreach { z => z should be (0) }
      tile.band(2).foreach { z => z should be (1) }
    }

    it("should read band interleave, tiled") {
      val tile =
        MultibandGeoTiff(p("tiled", "band")).tile

      tile.band(0).foreach { z => z should be (1) }
      tile.band(1).foreach { z => z should be (0) }
      tile.band(2).foreach { z => z should be (1) }
    }

  }
}
