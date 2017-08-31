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

import geotrellis.raster.io.geotiff.compression._
import geotrellis.raster.io.geotiff.tags.codes.ColorSpace
import geotrellis.raster.render.IndexedColorMap

/**
  * This case class holds information about how the data is stored in
  * a [[GeoTiff]]. If no values are given directly, then the defaults
  * are used.
  *
  * TODO: Use default parameters instead of constructor overloads in GeoTrellis 2.0
  */
case class GeoTiffOptions(
  storageMethod: StorageMethod,
  compression: Compression,
  colorSpace: Int,
  colorMap: Option[IndexedColorMap],
  interleaveMethod: InterleaveMethod
) {
  def this() = this(GeoTiffOptions.DEFAULT.storageMethod, GeoTiffOptions.DEFAULT.compression, GeoTiffOptions.DEFAULT.colorSpace, GeoTiffOptions.DEFAULT.colorMap, GeoTiffOptions.DEFAULT.interleaveMethod)

  def this(
    storageMethod: StorageMethod,
    compression: Compression,
    colorSpace: Int,
    colorMap: Option[IndexedColorMap]
   ) = this(
    storageMethod,
    compression,
    colorSpace,
    colorMap,
    GeoTiffOptions.DEFAULT.interleaveMethod
  )

  def this(
    storageMethod: StorageMethod,
    compression: Compression,
    interleaveMethod: InterleaveMethod
  ) = this(
    storageMethod,
    compression,
    GeoTiffOptions.DEFAULT.colorSpace,
    GeoTiffOptions.DEFAULT.colorMap,
    interleaveMethod
  )
}

/**
 * The companion object to [[GeoTiffOptions]]
 */
object GeoTiffOptions {
  val DEFAULT = GeoTiffOptions(Striped, NoCompression, ColorSpace.BlackIsZero, None, BandInterleave)

  def apply(): GeoTiffOptions = DEFAULT

  def apply(
    storageMethod: StorageMethod,
    compression: Compression,
    colorSpace: Int,
    colorMap: Option[IndexedColorMap]
  ): GeoTiffOptions = new GeoTiffOptions(storageMethod, compression, colorSpace, colorMap)

  def apply(storageMethod: StorageMethod, compression: Compression, interleaveMethod: InterleaveMethod): GeoTiffOptions =
    new GeoTiffOptions(storageMethod, compression, interleaveMethod)

  /**
   * Creates a new instance of [[GeoTiffOptions]] with the given
   * StorageMethod and the default compression value
   */
  def apply(storageMethod: StorageMethod): GeoTiffOptions =
    DEFAULT.copy(storageMethod = storageMethod)

  /**
   * Creates a new instance of [[GeoTiffOptions]] with the given
   * Compression and the default [[StorageMethod]] value
   */
  def apply(compression: Compression): GeoTiffOptions =
    DEFAULT.copy(compression = compression)

  /**
   * Creates a new instance of [[GeoTiffOptions]] with the given color map.
   */
  def apply(colorMap: IndexedColorMap): GeoTiffOptions =
    DEFAULT.copy(colorSpace = ColorSpace.Palette, colorMap = Some(colorMap))
}
