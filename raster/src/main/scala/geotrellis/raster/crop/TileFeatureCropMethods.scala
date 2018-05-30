/*
 * Copyright 2017 Azavea
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

package geotrellis.raster.crop

import geotrellis.raster._
import geotrellis.vector._
import geotrellis.util.MethodExtensions

class TileFeatureCropMethods[
    T <: CellGrid : (? => TileCropMethods[T]), 
    D
  ](val self: TileFeature[T, D]) extends TileCropMethods[TileFeature[T, D]] {
  import Crop.Options

  def crop(srcExtent: Extent, extent: Extent, options: Options): Option[TileFeature[T, D]] =
    self.tile.crop(srcExtent, extent, options).map(TileFeature(_, self.data))

  def crop(gb: GridBounds, options: Options): Option[TileFeature[T, D]] =
    self.tile.crop(gb, options).map(TileFeature(_, self.data))
}

class RasterTileFeatureCropMethods[
    T <: CellGrid : (? => TileCropMethods[T]),
    D
  ](val self: TileFeature[Raster[T], D]) extends TileCropMethods[TileFeature[Raster[T], D]] {
  import Crop.Options

  def crop(extent: Extent, options: Options): Option[TileFeature[Raster[T], D]] = {
    self.tile.crop(extent, options).map(TileFeature(_, self.data))
  }

  def crop(srcExtent: Extent, extent: Extent, options: Options): Option[TileFeature[Raster[T], D]] =
    self.tile.tile.crop(srcExtent, extent, options).map { tile => TileFeature(Raster(tile, extent), self.data) }

  /**
    * Given an Extent, produce a cropped [[Raster]].
    */
  def crop(extent: Extent): Option[TileFeature[Raster[T], D]] =
    crop(extent, Options.DEFAULT)

  /**
    * Given a [[GridBounds]] and some cropping options, produce a new
    * [[Raster]].
    */
  def crop(gb: GridBounds, options: Options): Option[TileFeature[Raster[T], D]] = {
    self.tile.crop(gb, options).map(TileFeature(_, self.data))
  }
}
