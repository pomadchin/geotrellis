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
import geotrellis.raster.merge._
import geotrellis.raster.prototype._
import geotrellis.raster.stitch._
import geotrellis.layer._
import geotrellis.vector.Extent
import geotrellis.util._

object Implicits extends Implicits

trait Implicits {
  implicit class withSpatialTileLayoutCollectionMethods[
    V <: CellGrid[Int]: Stitcher: ? => TilePrototypeMethods[V],
    M: GetComponent[?, LayoutDefinition]
  ](
    val self: Seq[(SpatialKey, V)] with Metadata[M]
  ) extends SpatialTileLayoutCollectionStitchMethods[V, M]

  implicit class withSpatialTileCollectionMethods[V <: CellGrid[Int]: Stitcher](
    val self: Seq[(SpatialKey, V)]
  ) extends SpatialTileCollectionStitchMethods[V]
}
