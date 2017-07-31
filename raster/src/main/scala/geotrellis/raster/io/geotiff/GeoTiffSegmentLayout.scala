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

import geotrellis.raster.GridBounds
import geotrellis.raster.TileLayout
import scala.collection.mutable.ArrayBuffer

/**
  * This case class represents how the segments in a given [[GeoTiff]] are arranged.
  *
  * @param totalCols          The total amount of cols in the GeoTiff
  * @param totalRows          The total amount of rows in the GeoTiff
  * @param tileLayout         The [[TileLayout]] of the GeoTiff
  * @param storagemethod      Storage method used for the segments (tiled or striped)
  * @param interleaveMethod   The interleave method used for segments (pixel or band)
  */
case class GeoTiffSegmentLayout(totalCols: Int, totalRows: Int, tileLayout: TileLayout, storageMethod: StorageMethod, interleaveMethod: InterleaveMethod) {
  def isTiled: Boolean =
    storageMethod match {
      case _: TiledStorageMethod => true
      case _ => false
    }

  def isStriped: Boolean = !isTiled

  def hasPixelInterleave: Boolean = interleaveMethod == PixelInterleave
}

trait GeoTiffSegmentLayoutTransform {
  private [geotiff] def segmentLayout: GeoTiffSegmentLayout
  private lazy val GeoTiffSegmentLayout(totalCols, totalRows, tileLayout, isTiled, interleaveMethod) =
    segmentLayout

  /** Count of the bands in the GeoTiff */
  def bandCount: Int

  /** Calculate the number of segments per band */
  private def bandSegmentCount =
    if(segmentLayout.hasPixelInterleave) {
      tileLayout.layoutCols * tileLayout.layoutRows
    } else {
      tileLayout.layoutCols * tileLayout.layoutRows / bandCount
    }

  /**
    * Calculates pixel dimensions of a given segment in this layout.
    * Segments are indexed in row-major order relative to the GeoTiff they comprise.
    *
    * @param segmentIndex: An Int that represents the given segment in the index
    * @return Tuple representing segment (cols, rows)
    */
  def getSegmentDimensions(segmentIndex: Int): (Int, Int) = {
    println(s"normalizedSegmentIndex = $segmentIndex % $bandSegmentCount // ${segmentLayout.hasPixelInterleave}")
    val normalizedSegmentIndex = segmentIndex// * bandSegmentCount
    val layoutCol = normalizedSegmentIndex % tileLayout.layoutCols
    val layoutRow = normalizedSegmentIndex / tileLayout.layoutCols

    val cols =
      if(layoutCol == tileLayout.layoutCols - 1) {
        totalCols - ( (tileLayout.layoutCols - 1) * tileLayout.tileCols)
      } else {
        tileLayout.tileCols
      }

    val rows =
      if(layoutRow == tileLayout.layoutRows - 1) {
        totalRows - ( (tileLayout.layoutRows - 1) * tileLayout.tileRows)
      } else {
        tileLayout.tileRows
      }

    (cols, rows)
  }

  /**
    * Finds the corresponding segment index given GeoTiff col and row.
    * If this is a band interleave geotiff, returns the segment index
    * for the first band.
    *
    * @param col  Pixel column in overall layout
    * @param row  Pixel row in overall layout
    * @return     The index of the segment in this layout
    */
  private [geotiff] def getSegmentIndex(col: Int, row: Int): Int = {
    val layoutCol = col / tileLayout.tileCols
    val layoutRow = row / tileLayout.tileRows
    (layoutRow * tileLayout.layoutCols) + layoutCol
  }

  private [geotiff] def getSegmentTransform(segmentIndex: Int): SegmentTransform = {
    val id = segmentIndex //% bandSegmentCount
    if (segmentLayout.isStriped)
      StripedSegmentTransform(id, GeoTiffSegmentLayoutTransform(segmentLayout, bandCount))
    else
      TiledSegmentTransform(id, GeoTiffSegmentLayoutTransform(segmentLayout, bandCount))
  }

  private [geotiff] def getGridBounds(segmentIndex: Int, isBit: Boolean = false): GridBounds = {
    val normalizedSegmentIndex = segmentIndex //% bandSegmentCount
    val (segmentCols, segmentRows) = getSegmentDimensions(normalizedSegmentIndex)

    val (startCol, startRow) = {
      val (layoutCol, layoutRow) =
        (normalizedSegmentIndex % tileLayout.layoutCols, segmentIndex / tileLayout.layoutCols)
      (layoutCol * tileLayout.tileCols, layoutRow * tileLayout.tileRows)
    }

    val endCol = (startCol + segmentCols) - 1
    val endRow = (startRow + segmentRows) - 1

    GridBounds(startCol, startRow, endCol, endRow)
  }

  /** Returns all segment indices which intersect given pixel grid bounds */
  private [geotiff] def getIntersectingSegments(bounds: GridBounds): Array[Int] = {
    val tc = tileLayout.tileCols
    val tr = tileLayout.tileRows
    val ab = ArrayBuffer[Int]()
    for (layoutCol <- (bounds.colMin / tc) to (bounds.colMax / tc)) {
      for (layoutRow <- (bounds.rowMin / tr) to (bounds.rowMax / tr)) {
        ab += (layoutRow * tileLayout.layoutCols) + layoutCol
      }
    }
    ab.toArray
  }

  /** Returns all segment indices which intersect given pixel grid bounds,
    * and for a subset of bands.
    * In a band interleave geotiff, generates the segment indices for the first band.
    *
    * @return  An array of (band index, segment index) tuples.
    */
  private [geotiff] def getIntersectingSegments(bounds: GridBounds, bands: Array[Int]): Array[(Int, Int)] = {
    val tc = tileLayout.tileCols
    val tr = tileLayout.tileRows
    val ab = ArrayBuffer[Int]()
    for (layoutCol <- (bounds.colMin / tc) to (bounds.colMax / tc)) {
      for (layoutRow <- (bounds.rowMin / tr) to (bounds.rowMax / tr)) {
        ab += (layoutRow * tileLayout.layoutCols) + layoutCol
      }
    }

    val firstBandSegments = getIntersectingSegments(bounds)
    bands.flatMap { band =>
      val segmentOffset = bandSegmentCount * band
      firstBandSegments.map { i => (band, i + segmentOffset) }
    }
  }
}

object GeoTiffSegmentLayoutTransform {
  def apply(_segmentLayout: GeoTiffSegmentLayout, _bandCount: Int): GeoTiffSegmentLayoutTransform =
    new GeoTiffSegmentLayoutTransform {
      val segmentLayout = _segmentLayout
      val bandCount = _bandCount
    }
}

/**
 * The companion object of [[GeoTiffSegmentLayout]]
 */
object GeoTiffSegmentLayout {
  /**
   * Given the totalCols, totalRows, storageMethod, and BandType of a GeoTiff,
   * a new instance of GeoTiffSegmentLayout will be created
   *
   * @param totalCols: The total amount of cols in the GeoTiff
   * @param totalRows: The total amount of rows in the GeoTiff
   * @param storageMethod: The [[StorageMethod]] of the GeoTiff
   * @param bandType: The [[BandType]] of the GeoTiff
   */
  def apply(
    totalCols: Int,
    totalRows: Int,
    storageMethod: StorageMethod,
    interleaveMethod: InterleaveMethod,
    bandType: BandType
  ): GeoTiffSegmentLayout = {
    val tileLayout =
      storageMethod match {
        case Tiled(blockCols, blockRows) =>
          val layoutCols = math.ceil(totalCols.toDouble / blockCols).toInt
          val layoutRows = math.ceil(totalRows.toDouble / blockRows).toInt
          TileLayout(layoutCols, layoutRows, blockCols, blockRows)
        case s: Striped =>
          val rowsPerStrip = math.min(s.rowsPerStrip(totalRows, bandType), totalRows).toInt
          val layoutRows = math.ceil(totalRows.toDouble / rowsPerStrip).toInt
          TileLayout(1, layoutRows, totalCols, rowsPerStrip)

      }
    GeoTiffSegmentLayout(totalCols, totalRows, tileLayout, storageMethod, interleaveMethod)
  }
}
