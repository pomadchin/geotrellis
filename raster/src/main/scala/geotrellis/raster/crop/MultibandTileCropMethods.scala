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

package geotrellis.raster.crop

import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.GeoTiffMultibandTile


/**
  * A trait containing crop methods for [[MultibandTile]]s.
  */
trait MultibandTileCropMethods extends TileCropMethods[MultibandTile] {
  import Crop.Options

  /**
    * Given a [[GridBounds]] and some cropping options, crop the
    * [[MultibandTile]] and return a new MultibandTile.
    */
  def crop(gb: GridBounds, options: Options): Option[MultibandTile] = {
    if(gb.intersects(self.gridBounds)) {
      self match {
        case geotiffTile: GeoTiffMultibandTile =>
          val cropBounds =
            if (options.clamp) gb.intersection(self)
            else Some(gb)
          cropBounds.flatMap(geotiffTile.crop)
        case _ =>
          val croppedBands = Array.ofDim[Option[Tile]](self.bandCount)
          for (b <- 0 until self.bandCount) {
            croppedBands(b) = self.band(b).crop(gb, options)
          }
          Some(ArrayMultibandTile(croppedBands.flatten))
      }
    } else None
  }

  /**
    * Given a source Extent (the extent of the present
    * [[MultibandTile]]), a destination Extent, and a set of Options,
    * return a new MultibandTile.
    */
  def crop(srcExtent: Extent, extent: Extent, options: Options): Option[MultibandTile] =
    Raster(self, srcExtent).crop(extent, options).map(_.tile)
}
