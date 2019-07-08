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

package geotrellis.layer.stitch

import geotrellis.raster._
import geotrellis.raster.stitch.Stitcher
import geotrellis.layer._
import geotrellis.layer.Implicits._
import geotrellis.raster.prototype._
import geotrellis.vector.Extent
import geotrellis.util._

abstract class SpatialTileLayoutCollectionStitchMethods[
  V <: CellGrid[Int]: Stitcher: ? => TilePrototypeMethods[V],
  M: GetComponent[?, LayoutDefinition]
] extends MethodExtensions[Seq[(SpatialKey, V)] with Metadata[M]] {

  def stitch(): Raster[V] = {
    val (tile, (kx, ky), (offsx, offsy)) = TileLayoutStitcher.stitch(self)
    val layout = self.metadata.getComponent[LayoutDefinition]
    val mapTransform = layout.mapTransform
    val nwTileEx = mapTransform(kx, ky)
    val base = nwTileEx.southEast
    val (ulx, uly) = (base.x - offsx.toDouble * layout.cellwidth, base.y + offsy * layout.cellheight)
    Raster(tile, Extent(ulx, uly - tile.rows * layout.cellheight, ulx + tile.cols * layout.cellwidth, uly))
  }

  /**
    * Stitch tiles in collection across an extent
    *
    * This method performs a sparse stitch, filling any missing tiles within the
    * extent with an empty prototype tile.
    *
    * @param extent
    * @return
    */
  def stitch(extent: Extent): Option[Raster[V]] = {
    if (self.headOption.isEmpty) {
      None
    } else {
      val tile = self.head._2
      val layoutDefinition = self.metadata.getComponent[LayoutDefinition]
      val mapTransform = layoutDefinition.mapTransform
      val expectedKeys = mapTransform(extent)
        .coordsIter
        .map { case (x, y) => SpatialKey(x, y) }
        .toList
      val actualKeys = self.map(_._1)
      val missingKeys = expectedKeys diff actualKeys

      val missingTiles = missingKeys.map { key =>
        (key, tile.prototype(layoutDefinition.tileLayout.tileCols, layoutDefinition.tileLayout.tileRows))
      }
      val allTiles = self.withContext { collection =>
        collection.toList ::: missingTiles
      }
      if (allTiles.isEmpty) {
        None
      } else {
        Some(allTiles.stitch())
      }
    }
  }
}

abstract class SpatialTileCollectionStitchMethods[V <: CellGrid[Int]: Stitcher]
  extends MethodExtensions[Seq[(SpatialKey, V)]] {

  def stitch(): V = TileLayoutStitcher.stitch(self)._1
}
