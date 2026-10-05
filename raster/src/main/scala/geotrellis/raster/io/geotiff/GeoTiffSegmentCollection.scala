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

import geotrellis.raster.io.geotiff.compression.*

trait GeoTiffSegmentCollection {
  type T >: Null <: GeoTiffSegment

  /** represents all of the segments in the geotiff */
  val segmentBytes: SegmentBytes
  val decompressor: Decompressor

  val bandType: BandType

  def decompressGeoTiffSegment: (Int, Array[Byte]) => T

  // Cached last segment, index and segment are published together so that concurrent readers never mix them up
  @volatile private var _lastSegment: (Int, T) = null

  def getSegment(i: Int): T = {
    val last = _lastSegment
    if (last != null && last._1 == i) last._2
    else {
      val segment = decompressGeoTiffSegment(i, segmentBytes.getSegment(i))
      _lastSegment = (i, segment)
      segment
    }
  }

  def getSegments(ids: Iterable[Int]): Iterator[(Int, T)] = {
    for { (id, bytes) <- segmentBytes.getSegments(ids) }
      yield id -> decompressGeoTiffSegment(id, bytes)
  }
}
