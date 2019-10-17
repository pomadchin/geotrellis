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

package geotrellis.raster.io.geotiff

import geotrellis.raster._

import geotrellis.raster.testkit._

import org.scalatest._

class BitGeoTiffTileSpec extends FunSpec
    with Matchers
    with RasterMatchers
    with BeforeAndAfterAll
    with GeoTiffTestUtils 
    with TileBuilders {
  describe("BitGeoTiffTile") {
    it("should map same") {
      val tile = SinglebandGeoTiff(geoTiffPath("bilevel.tif")).tile

      val res = tile.map { z => z }

      assertEqual(res, tile)
    }

    it("should map inverse") {
      val tile = SinglebandGeoTiff(geoTiffPath("bilevel.tif")).tile

      val res = tile.map { z => z ^ 1 }.map { z => z ^ 1 }

      assertEqual(res, tile)
    }

    it("should map with index") {
      val tile = SinglebandGeoTiff(geoTiffPath("bilevel.tif")).tile

      val res = tile.map { (col, row, z) => z }

      assertEqual(res, tile)
    }

    it("should map with index double") {
      val tile = SinglebandGeoTiff(geoTiffPath("bilevel.tif")).tile

      val res = tile.mapDouble { (col, row, z) => z }

      assertEqual(res, tile)
    }

    it("should map with index - tiled") {
      val tile = SinglebandGeoTiff(geoTiffPath("bilevel_tiled.tif")).tile

      val res = tile.map { (col, row, z) => z }

      assertEqual(res, tile)
    }

    it("should map with index double - tiled") {
      val tile = SinglebandGeoTiff(geoTiffPath("bilevel_tiled.tif")).tile

      val res = tile.mapDouble { (col, row, z) => z }

      assertEqual(res, tile)
    }

    it("should convert tiled tiffs") {
      val tiff = SinglebandGeoTiff(geoTiffPath("bilevel.tif"))
      val tile = tiff.tile.toArrayTile()

      // check that it is possible to convert bit cellType to bit cellType
      val tiffTile = tile.toGeoTiffTile.convert(BitCellType)
      assertEqual(tiffTile.toArrayTile(), tile.toArrayTile)

      // check that it is possible to convert int cellType to bit cellType
      // and that bitCellType conversion is idempotent
      (0 to 5).foldLeft(tile.toGeoTiffTile.convert(IntCellType)) { case (acc, _) =>
        val tiffTileLocal = acc.convert(BitCellType)
        assertEqual(tiffTileLocal.toArrayTile(), tile.toArrayTile)
        tiffTileLocal
      }
    }

    it("should convert striped tiffs (1 row per strip)") {
      val tiff = SinglebandGeoTiff("/Users/daunnc/Downloads/bilevel-strip-2.tif")
      // val tiff = SinglebandGeoTiff("/Users/daunnc/Downloads/bilevel-strip-t3.tif")
      // val tiff = SinglebandGeoTiff(geoTiffPath("3bands/bit/3bands-striped-band.tif"))
      val tile = tiff.tile // .toArrayTile()

      // check that it is possible to convert bit cellType to bit cellType
      val tiffTile = tile/*.toGeoTiffTile*/.convert(BitCellType)
      assertEqual(tiffTile.toArrayTile(), tile.toArrayTile)

      // check that it is possible to convert int cellType to bit cellType
      // and that bitCellType conversion is idempotent
      (0 to 5).foldLeft(tile/*.toGeoTiffTile*/.convert(IntCellType)) { case (acc, _) =>
        val tiffTileLocal = acc.convert(BitCellType)
        assertEqual(tiffTileLocal.toArrayTile(), tile.toArrayTile)
        tiffTileLocal
      }
    }

    it("should convert striped tiffs (3 rows per strip)") {
      /**
        * col, row: (0,4)
        * segmentIndex: 0
        * i: 3640
        *
        * col: 3640 % 912: 904
        * row: 3640 / 912: 3
        * index: (3 * 912) + 904: 3640
        * index(3640): 3640
        * col: 3640 % 912: 904
        * row: 3640 / 912: 3
        * index: (3 * 912) + 904: 3640
        * index(3640) >> 3: 455
        * col: 3640 % 912: 904
        * row: 3640 / 912: 3
        * index: (3 * 912) + 904: 3640
        * bytes(index >> 3): -20
        *
        * col: 3640 % 912: 904
        * row: 3640 / 912: 3
        * index: (3 * 912) + 904: 3640
        * 1
        * col, row: (0,4)
        * segmentIndex: 1
        * i: 910
        *
        * col: 910 % 912: 910
        * row: 910 / 912: 0
        * index: (0 * 912) + 910: 910
        * index(910): 910
        * col: 910 % 912: 910
        * row: 910 / 912: 0
        * index: (0 * 912) + 910: 910
        * index(910) >> 3: 113
        * col: 910 % 912: 910
        * row: 910 / 912: 0
        * index: (0 * 912) + 910: 910
        * bytes(index >> 3): -20
        *
        * col: 910 % 912: 910
        * row: 910 / 912: 0
        * index: (0 * 912) + 910: 910
        * 0
        * col, row: (1,4)
        * segmentIndex: 0
        * i: 3641
        *
        * col: 3641 % 912: 905
        * row: 3641 / 912: 3
        * index: (3 * 912) + 905: 3641
        * index(3641): 3641
        * col: 3641 % 912: 905
        * row: 3641 / 912: 3
        * index: (3 * 912) + 905: 3641
        * index(3641) >> 3: 455
        * col: 3641 % 912: 905
        * row: 3641 / 912: 3
        * index: (3 * 912) + 905: 3641
        * bytes(index >> 3): -20
        *
        * col: 3641 % 912: 905
        * row: 3641 / 912: 3
        * index: (3 * 912) + 905: 3641
        * 1
        * col, row: (1,4)
        * segmentIndex: 1
        * i: 911
        *
        * col: 911 % 912: 911
        * row: 911 / 912: 0
        * index: (0 * 912) + 911: 911
        * index(911): 911
        * col: 911 % 912: 911
        * row: 911 / 912: 0
        * index: (0 * 912) + 911: 911
        * index(911) >> 3: 113
        * col: 911 % 912: 911
        * row: 911 / 912: 0
        * index: (0 * 912) + 911: 911
        * bytes(index >> 3): -20
        *
        * col: 911 % 912: 911
        * row: 911 / 912: 0
        * index: (0 * 912) + 911: 911
        * 0
        * col, row: (0,5)
        * segmentIndex: 0
        * i: 4550
        *
        * col: 4550 % 912: 902
        * row: 4550 / 912: 4
        * index: (4 * 912) + 902: 4550
        * index(4550): 4550
        * col: 4550 % 912: 902
        * row: 4550 / 912: 4
        * index: (4 * 912) + 902: 4550
        * index(4550) >> 3: 568
        * col: 4550 % 912: 902
        * row: 4550 / 912: 4
        * index: (4 * 912) + 902: 4550
        * bytes(index >> 3): -1
        *
        * col: 4550 % 912: 902
        * row: 4550 / 912: 4
        * index: (4 * 912) + 902: 4550
        * 1
        * col, row: (0,5)
        * segmentIndex: 1
        * i: 1820
        *
        * col: 1820 % 912: 908
        * row: 1820 / 912: 1
        * index: (1 * 912) + 908: 1820
        * index(1820): 1820
        * col: 1820 % 912: 908
        * row: 1820 / 912: 1
        * index: (1 * 912) + 908: 1820
        * index(1820) >> 3: 227
        * col: 1820 % 912: 908
        * row: 1820 / 912: 1
        * index: (1 * 912) + 908: 1820
        * bytes(index >> 3): -20
        *
        * col: 1820 % 912: 908
        * row: 1820 / 912: 1
        * index: (1 * 912) + 908: 1820
        * 1
        * col, row: (1,5)
        * segmentIndex: 0
        * i: 4551
        *
        * col: 4551 % 912: 903
        * row: 4551 / 912: 4
        * index: (4 * 912) + 903: 4551
        * index(4551): 4551
        * col: 4551 % 912: 903
        * row: 4551 / 912: 4
        * index: (4 * 912) + 903: 4551
        * index(4551) >> 3: 568
        * col: 4551 % 912: 903
        * row: 4551 / 912: 4
        * index: (4 * 912) + 903: 4551
        * bytes(index >> 3): -1
        *
        * col: 4551 % 912: 903
        * row: 4551 / 912: 4
        * index: (4 * 912) + 903: 4551
        * 1
        * col, row: (1,5)
        * segmentIndex: 1
        * i: 1821
        *
        * col: 1821 % 912: 909
        * row: 1821 / 912: 1
        * index: (1 * 912) + 909: 1821
        * index(1821): 1821
        * col: 1821 % 912: 909
        * row: 1821 / 912: 1
        * index: (1 * 912) + 909: 1821
        * index(1821) >> 3: 227
        * col: 1821 % 912: 909
        * row: 1821 / 912: 1
        * index: (1 * 912) + 909: 1821
        * bytes(index >> 3): -20
        *
        * col: 1821 % 912: 909
        * row: 1821 / 912: 1
        * index: (1 * 912) + 909: 1821
        * 1
        *
        * --------------------
        *
        * col, row: (0,4)
        * segmentIndex: 0
        * i: 3640
        *
        * col: 3640 % 910: 0
        * row: 3640 / 910: 4
        * index: (4 * 912) + 0: 3648
        * index(3640): 3648
        * col: 3640 % 910: 0
        * row: 3640 / 910: 4
        * index: (4 * 912) + 0: 3648
        * index(3640) >> 3: 456
        * col: 3640 % 910: 0
        * row: 3640 / 910: 4
        * index: (4 * 912) + 0: 3648
        * bytes(index >> 3): -33
        *
        * col: 3640 % 910: 0
        * row: 3640 / 910: 4
        * index: (4 * 912) + 0: 3648
        * 1
        * col, row: (0,4)
        * segmentIndex: 1
        * i: 910
        *
        * col: 910 % 910: 0
        * row: 910 / 910: 1
        * index: (1 * 912) + 0: 912
        * index(910): 912
        * col: 910 % 910: 0
        * row: 910 / 910: 1
        * index: (1 * 912) + 0: 912
        * index(910) >> 3: 114
        * col: 910 % 910: 0
        * row: 910 / 910: 1
        * index: (1 * 912) + 0: 912
        * bytes(index >> 3): -33
        *
        * col: 910 % 910: 0
        * row: 910 / 910: 1
        * index: (1 * 912) + 0: 912
        * 1
        * col, row: (1,4)
        * segmentIndex: 0
        * i: 3641
        *
        * col: 3641 % 910: 1
        * row: 3641 / 910: 4
        * index: (4 * 912) + 1: 3649
        * index(3641): 3649
        * col: 3641 % 910: 1
        * row: 3641 / 910: 4
        * index: (4 * 912) + 1: 3649
        * index(3641) >> 3: 456
        * col: 3641 % 910: 1
        * row: 3641 / 910: 4
        * index: (4 * 912) + 1: 3649
        * bytes(index >> 3): -33
        *
        * col: 3641 % 910: 1
        * row: 3641 / 910: 4
        * index: (4 * 912) + 1: 3649
        * 1
        * col, row: (1,4)
        * segmentIndex: 1
        * i: 911
        *
        * col: 911 % 910: 1
        * row: 911 / 910: 1
        * index: (1 * 912) + 1: 913
        * index(911): 913
        * col: 911 % 910: 1
        * row: 911 / 910: 1
        * index: (1 * 912) + 1: 913
        * index(911) >> 3: 114
        * col: 911 % 910: 1
        * row: 911 / 910: 1
        * index: (1 * 912) + 1: 913
        * bytes(index >> 3): -33
        *
        * col: 911 % 910: 1
        * row: 911 / 910: 1
        * index: (1 * 912) + 1: 913
        * 1
        * col, row: (0,5)
        * segmentIndex: 0
        * i: 4550
        *
        * col: 4550 % 910: 0
        * row: 4550 / 910: 5
        * index: (5 * 912) + 0: 4560
        * index(4550): 4560
        * col: 4550 % 910: 0
        * row: 4550 / 910: 5
        * index: (5 * 912) + 0: 4560
        * index(4550) >> 3: 570
        * col: 4550 % 910: 0
        * row: 4550 / 910: 5
        * index: (5 * 912) + 0: 4560
        * bytes(index >> 3): -33
        *
        * col: 4550 % 910: 0
        * row: 4550 / 910: 5
        * index: (5 * 912) + 0: 4560
        * 1
        * col, row: (0,5)
        * segmentIndex: 1
        * i: 1820
        *
        * col: 1820 % 910: 0
        * row: 1820 / 910: 2
        * index: (2 * 912) + 0: 1824
        * index(1820): 1824
        * col: 1820 % 910: 0
        * row: 1820 / 910: 2
        * index: (2 * 912) + 0: 1824
        * index(1820) >> 3: 228
        * col: 1820 % 910: 0
        * row: 1820 / 910: 2
        * index: (2 * 912) + 0: 1824
        * bytes(index >> 3): -33
        *
        * col: 1820 % 910: 0
        * row: 1820 / 910: 2
        * index: (2 * 912) + 0: 1824
        * 1
        * col, row: (1,5)
        * segmentIndex: 0
        * i: 4551
        *
        * col: 4551 % 910: 1
        * row: 4551 / 910: 5
        * index: (5 * 912) + 1: 4561
        * index(4551): 4561
        * col: 4551 % 910: 1
        * row: 4551 / 910: 5
        * index: (5 * 912) + 1: 4561
        * index(4551) >> 3: 570
        * col: 4551 % 910: 1
        * row: 4551 / 910: 5
        * index: (5 * 912) + 1: 4561
        * bytes(index >> 3): -33
        *
        * col: 4551 % 910: 1
        * row: 4551 / 910: 5
        * index: (5 * 912) + 1: 4561
        * 1
        * col, row: (1,5)
        * segmentIndex: 1
        * i: 1821
        *
        * col: 1821 % 910: 1
        * row: 1821 / 910: 2
        * index: (2 * 912) + 1: 1825
        * index(1821): 1825
        * col: 1821 % 910: 1
        * row: 1821 / 910: 2
        * index: (2 * 912) + 1: 1825
        * index(1821) >> 3: 228
        * col: 1821 % 910: 1
        * row: 1821 / 910: 2
        * index: (2 * 912) + 1: 1825
        * bytes(index >> 3): -33
        *
        * col: 1821 % 910: 1
        * row: 1821 / 910: 2
        * index: (2 * 912) + 1: 1825
        * 1
        */

      val tiled = SinglebandGeoTiff(geoTiffPath("bilevel.tif")).tile//.convert(IntCellType)
      val tiff = SinglebandGeoTiff("/Users/daunnc/Downloads/bilevel-strip.tif")

      //println((tiled.tile - tiff.tile).findMinMax)
      println(tiled.get(0, 4))
      println(tiff.tile.get(0, 4))
      println(tiled.get(1, 4))
      println(tiff.tile.get(1, 4))

      println(tiled.get(0, 5))
      println(tiff.tile.get(0, 5))
      println(tiled.get(1, 5))
      println(tiff.tile.get(1, 5))

      // val tiff = SinglebandGeoTiff(geoTiffPath("3bands/bit/3bands-striped-band.tif"))
      val tile = tiff.tile // .toArrayTile()

      // check that it is possible to convert bit cellType to bit cellType
      val tiffTile = tile/*.toGeoTiffTile*///.convert(BitCellType)// .convert(BitCellType)
      // GeoTiff(tiffTile, )

      // assertEqual(tiffTile, tiled.tile)
      assertEqual(tiffTile.toArrayTile(), tiled.tile.toArrayTile)
      // assertEqual(tiffTile.toArrayTile(), tile.toArrayTile)

      // assertEqual(tiffTile.toArrayTile(), tiled.tile.toArrayTile)

      // val tiffTile2 = tile.toGeoTiffTile(GeoTiffOptions.DEFAULT.copy(storageMethod = Striped)).convert(BitCellType)
      // SinglebandGeoTiff(tiffTile2, tiff.extent, tiff.crs, Tags.empty, GeoTiffOptions.DEFAULT.copy(storageMethod = Striped))
      //  .write("/Users/daunnc/Downloads/bilevel-striped-again-11l.tiff")

      // check that it is possible to convert int cellType to bit cellType
      // and that bitCellType conversion is idempotent
      /*(0 to 5).foldLeft(tile/*.toGeoTiffTile*/.convert(IntCellType)) { case (acc, _) =>
        val tiffTileLocal = acc.convert(BitCellType)
        assertEqual(tiffTileLocal.toArrayTile(), tile.toArrayTile)
        tiffTileLocal
      }*/
    }

    /*it("should convert striped tiffs2") {
      val tiff = SinglebandGeoTiff(geoTiffPath("bilevel.tif"))
      val tile = tiff.tile.toArrayTile()

      // check that it is possible to convert bit cellType to bit cellType
      val tiffTile = tile.toGeoTiffTile(GeoTiffOptions.DEFAULT.copy(storageMethod = Striped)).convert(BitCellType)
      SinglebandGeoTiff(tiffTile, tiff.extent, tiff.crs, Tags.empty, GeoTiffOptions.DEFAULT.copy(storageMethod = Striped))
        .write("/Users/daunnc/Downloads/bilevel-striped-again-1.tiff")

      // assertEqual(tiffTile.toArrayTile(), tile.toArrayTile)

      // check that it is possible to convert int cellType to bit cellType
      // and that bitCellType conversion is idempotent
      /*(0 to 5).foldLeft(tile.toGeoTiffTile.convert(IntCellType)) { case (acc, _) =>
        val tiffTileLocal = acc.convert(BitCellType)
        assertEqual(tiffTileLocal.toArrayTile(), tile.toArrayTile)
        tiffTileLocal
      }*/
    }*/
  }
}
