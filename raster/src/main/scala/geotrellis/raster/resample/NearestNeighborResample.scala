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

package geotrellis.raster.resample

import geotrellis.raster._
import geotrellis.vector.Extent

class NearestNeighborResample(tile: Tile, extent: Extent)
    extends Resample(tile, extent) {

  override def resampleValid(x: Double, y: Double): Int = {
    val (col, row) = {
      val (bc, br) = re.coord2pixel(x, y)
      val c = if(bc < tile.cols) bc else tile.cols - 1
      val r = if(br < tile.rows) br else tile.rows - 1
      c -> r
    }
    /*val col = {
      val c = re.mapXToGrid(x)
      if(c < tile.cols) c else tile.cols - 1
    }

    val row = {
      val r = re.mapYToGrid(y)
      if(r < tile.rows) r else tile.rows - 1
    }*/

    tile.get(col, row)
  }

  override def resampleDoubleValid(x: Double, y: Double): Double = {
    val col = {
      val c = re.mapXToGrid(x)
      if(c < tile.cols) c else tile.cols - 1
    }

    val row = {
      val r = re.mapYToGrid(y)
      if(r < tile.rows) r else tile.rows - 1
    }

    tile.getDouble(col, row)
  }

}
