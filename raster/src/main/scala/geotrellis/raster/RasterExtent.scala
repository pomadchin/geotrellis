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

package geotrellis.raster

import geotrellis.vector.{Extent, Point}
import scala.math.{min, max, ceil}


/**
  * [[GeoAttrsError]] exception.
  */
case class GeoAttrsError(msg: String) extends Exception(msg)

/**
  * [[RasterExtent]] objects represent the geographic extent
  * (envelope) of a raster.
  *
  * The Raster extent has two coordinate concepts involved: map
  * coordinates and grid coordinates. Map coordinates are what the
  * Extent class uses, and specifies points using an X coordinate and
  * a Y coordinate. The X coordinate is oriented along west to east
  * such that the larger the X coordinate, the more eastern the
  * point. The Y coordinate is along south to north such that the
  * larger the Y coordinate, the more Northern the point.
  *
  * This contrasts with the grid coordinate system. The grid
  * coordinate system does not actually reference points on the map,
  * but instead a cell of the raster that represents values for some
  * square area of the map. The column axis is similar in that the
  * number gets larger as one goes from west to east; however, the row
  * axis is inverted from map coordinates: as the row number
  * increases, the cell is heading south. The top row is labeled as 0,
  * and the next 1, so that the highest indexed row is the southern
  * most row of the raster.  A cell has a height and a width that is
  * in terms of map units. You can think of it as each cell is itself
  * an extent, with width cellwidth and height cellheight. When a cell
  * needs to be represented or thought of as a point, the center of
  * the cell will be used.  So when gridToMap is called, what is
  * returned is the center point, in map coordinates.
  *
  * Map points are considered to be 'inside' the cell based on these
  * rules:
  *  - If the point is inside the area of the cell, it is included in
  *    the cell.
  *  - If the point lies on the north or west border of the cell, it
  *    is included in the cell.
  *  - If the point lies on the south or east border of the cell, it
  *    is not included in the cell, it is included in the next
  *    southern or eastern cell, respectively.
  *
  * Note that based on these rules, the eastern and southern borders
  * of an Extent are not actually considered to be part of the
  * RasterExtent.
  */
case class RasterExtent(
  override val extent: Extent,
  override val cellwidth: Double,
  override val cellheight: Double,
  cols: Int,
  rows: Int
) extends GridExtent(extent, cellwidth, cellheight) with Grid {

  if (cols <= 0) throw GeoAttrsError(s"invalid cols: $cols")
  if (rows <= 0) throw GeoAttrsError(s"invalid rows: $rows")

  /**
    * Convert map coordinates (x, y) to grid coordinates (col, row).
    */
  final def mapToGrid(x: Double, y: Double): (Int, Int) = {
    val col = math.floor((x - extent.xmin) / cellwidth).toInt
    val row = math.floor((extent.ymax - y) / cellheight).toInt
    (col, row)
  }

  /**
    * Convert map coordinate x to grid coordinate column.
    */
  final def mapXToGrid(x: Double): Int = math.floor(mapXToGridDouble(x)).toInt

  /**
    * Convert map coordinate x to grid coordinate column.
    */
  final def mapXToGridDouble(x: Double): Double = (x - extent.xmin) / cellwidth

  /**
    * Convert map coordinate y to grid coordinate row.
    */
  final def mapYToGrid(y: Double): Int = math.floor(mapYToGridDouble(y)).toInt

  /**
    * Convert map coordinate y to grid coordinate row.
    */
  final def mapYToGridDouble(y: Double): Double = (extent.ymax - y ) / cellheight

  /**
    * Convert map coordinate tuple (x, y) to grid coordinates (col, row).
    */
  final def mapToGrid(mapCoord: (Double, Double)): (Int, Int) = {
    val (x, y) = mapCoord
    mapToGrid(x, y)
  }

  /**
   * Convert a point to grid coordinates (col, row).
   */
  final def mapToGrid(p: Point): (Int, Int) =
    mapToGrid(p.x, p.y)

  /**
    * The map coordinate of a grid cell is the center point.
    */
  final def gridToMap(col: Int, row: Int): (Double, Double) = {
    val x = col * cellwidth + extent.xmin + (cellwidth / 2)
    val y = extent.ymax - (row * cellheight) - (cellheight / 2)

    (x, y)
  }

  /**
    * For a give column, find the corresponding x-coordinate in the
    * grid of the present [[RasterExtent]].
    */
  final def gridColToMap(col: Int): Double = {
    col * cellwidth + extent.xmin + (cellwidth / 2)
  }

  /**
    * For a give row, find the corresponding y-coordinate in the grid
    * of the present [[RasterExtent]].
    */
  final def gridRowToMap(row: Int): Double = {
    extent.ymax - (row * cellheight) - (cellheight / 2)
  }

  /**
    * Gets the GridBounds aligned with this RasterExtent that is the
    * smallest subgrid of containing all points within the extent. The
    * extent is considered inclusive on it's north and west borders,
    * exclusive on it's east and south borders.  See [[RasterExtent]]
    * for a discussion of grid and extent boundary concepts.
    *
    * The 'clamp' flag determines whether or not to clamp the
    * GridBounds to the RasterExtent; defaults to true. If false,
    * GridBounds can contain negative values, or values outside of
    * this RasterExtent's boundaries.
    *
    * @param     subExtent      The extent to get the grid bounds for
    * @param     clamp          A boolean
    */
  def gridBoundsFor(subExtent: Extent, clamp: Boolean = true): GridBounds = {
    // West and North boundaries are a simple mapToGrid call.
    val (colMin, rowMin) = mapToGrid(subExtent.xmin, subExtent.ymax)

    // If South East corner is on grid border lines, we want to still only include
    // what is to the West and\or North of the point. However if the border point
    // is not directly on a grid division, include the whole row and/or column that
    // contains the point.
    val colMax = {
      val colMaxDouble = mapXToGridDouble(subExtent.xmax)
      if(math.abs(colMaxDouble - math.floor(colMaxDouble)) < RasterExtent.epsilon) colMaxDouble.toInt - 1
      else colMaxDouble.toInt
    }

    val rowMax = {
      val rowMaxDouble = mapYToGridDouble(subExtent.ymin)
      if(math.abs(rowMaxDouble - math.floor(rowMaxDouble)) < RasterExtent.epsilon) rowMaxDouble.toInt - 1
      else rowMaxDouble.toInt
    }

    if(clamp) {
      GridBounds(math.min(math.max(colMin, 0), cols - 1),
                 math.min(math.max(rowMin, 0), rows - 1),
                 math.min(math.max(colMax, 0), cols - 1),
                 math.min(math.max(rowMax, 0), rows - 1))
    } else {
      GridBounds(colMin, rowMin, colMax, rowMax)
    }
  }

  /**
    * Combine two different [[RasterExtent]]s (which must have the
    * same cellsizes).  The result is a new extent at the same
    * resolution.
    */
  def combine (that: RasterExtent): RasterExtent = {
    if (cellwidth != that.cellwidth)
      throw GeoAttrsError(s"illegal cellwidths: $cellwidth and ${that.cellwidth}")
    if (cellheight != that.cellheight)
      throw GeoAttrsError(s"illegal cellheights: $cellheight and ${that.cellheight}")

    val newExtent = extent.combine(that.extent)
    val newRows = ceil(newExtent.height / cellheight).toInt
    val newCols = ceil(newExtent.width / cellwidth).toInt

    RasterExtent(newExtent, cellwidth, cellheight, newCols, newRows)
  }

  /**
    * Returns a [[RasterExtent]] with the same extent, but a modified
    * number of columns and rows based on the given cell height and
    * width.
    */
  def withResolution(targetCellWidth: Double, targetCellHeight: Double): RasterExtent = {
    val newCols = math.ceil((extent.xmax - extent.xmin) / targetCellWidth).toInt
    val newRows = math.ceil((extent.ymax - extent.ymin) / targetCellHeight).toInt
    RasterExtent(extent, targetCellWidth, targetCellHeight, newCols, newRows)
  }

  /**
    * Returns a [[RasterExtent]] with the same extent, but a modified
    * number of columns and rows based on the given cell height and
    * width.
    */
  def withResolution(cellSize: CellSize): RasterExtent =
    withResolution(cellSize.width, cellSize.height)

  /**
   * Returns a [[RasterExtent]] with the same extent and the given
   * number of columns and rows.
   */
  def withDimensions(targetCols: Int, targetRows: Int): RasterExtent =
    RasterExtent(extent, targetCols, targetRows)

  /**
    * Adjusts a raster extent so that it can encompass the tile
    * layout.  Will resample the extent, but keep the resolution, and
    * preserve north and west borders
    */
  def adjustTo(tileLayout: TileLayout): RasterExtent = {
    val totalCols = tileLayout.tileCols * tileLayout.layoutCols
    val totalRows = tileLayout.tileRows * tileLayout.layoutRows

    val resampledExtent = Extent(extent.xmin, extent.ymax - (cellheight*totalRows),
                        extent.xmin + (cellwidth*totalCols), extent.ymax)

    RasterExtent(resampledExtent, cellwidth, cellheight, totalCols, totalRows)
  }

  /**
    * Returns a new [[RasterExtent]] which represents the GridBounds
    * in relation to this RasterExtent.
    */
  def rasterExtentFor(gridBounds: GridBounds): RasterExtent = {
    val (xminCenter, ymaxCenter) = gridToMap(gridBounds.colMin, gridBounds.rowMin)
    val (xmaxCenter, yminCenter) = gridToMap(gridBounds.colMax, gridBounds.rowMax)
    val (hcw, hch) = (cellwidth / 2, cellheight / 2)
    val e = Extent(xminCenter - hcw, yminCenter - hch, xmaxCenter + hcw, ymaxCenter + hch)
    RasterExtent(e, cellwidth, cellheight, gridBounds.width, gridBounds.height)
  }
}

/**
  * The companion object for the [[RasterExtent]] type.
  */
object RasterExtent {
  final val epsilon = 0.0000001

  /**
    * Create a new [[RasterExtent]] from an Extent, a column, and a
    * row.
    */
  def apply(extent: Extent, cols: Int, rows: Int): RasterExtent = {
    val cw = extent.width / cols
    val ch = extent.height / rows
    RasterExtent(extent, cw, ch, cols, rows)
  }

  /**
    * Create a new [[RasterExtent]] from an Extent and a [[CellSize]].
    */
  def apply(extent: Extent, cellSize: CellSize): RasterExtent = {
    val cols = (extent.width / cellSize.width).toInt
    val rows = (extent.height / cellSize.height).toInt
    RasterExtent(extent, cellSize.width, cellSize.height, cols, rows)
  }

  /**
    * Create a new [[RasterExtent]] from a [[CellGrid]] and an Extent.
    */
  def apply(tile: CellGrid, extent: Extent): RasterExtent =
    apply(extent, tile.cols, tile.rows)

  /**
    * Create a new [[RasterExtent]] from an Extent and a [[CellGrid]].
    */
  def apply(extent: Extent, tile: CellGrid): RasterExtent =
    apply(extent, tile.cols, tile.rows)
}

