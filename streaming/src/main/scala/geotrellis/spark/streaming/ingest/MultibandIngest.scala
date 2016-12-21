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

package geotrellis.spark.streaming.ingest

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.resample.{NearestNeighbor, ResampleMethod}
import geotrellis.spark._
import geotrellis.spark.ingest.{MultibandIngest => RDDMultibandIngest}
import geotrellis.spark.tiling._
import geotrellis.util._
import geotrellis.vector._

import org.apache.spark.Partitioner
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

object MultibandIngest {
  /**
   * Represents the ingest process.
   * An ingest process produces a layer from a set of input rasters.
   *
   * The ingest process has the following steps:
   *
   *  - Reproject tiles to the desired CRS:  (CRS, RDD[(Extent, CRS), Tile)]) -> RDD[(Extent, Tile)]
   *  - Determine the appropriate layer meta data for the layer. (CRS, LayoutScheme, RDD[(Extent, Tile)]) -> LayerMetadata)
   *  - Resample the rasters into the desired tile format. RDD[(Extent, Tile)] => TileLayerRDD[K]
   *  - Optionally pyramid to top zoom level, calling sink at each level
   *
   * Ingesting is abstracted over the following variants:
   *  - The source of the input tiles, which are represented as an RDD of (T, Tile) tuples, where T: Component[?, ProjectedExtent]
   *  - The LayoutScheme which will be used to determine how to retile the input tiles.
   *
   * @param sourceTiles   DStream of tiles that have Extent and CRS
   * @param destCRS       CRS to be used by the output layer
   * @param layoutScheme  LayoutScheme to be used by output layer
   * @param pyramid       Pyramid up to level 1, sink function will be called for each level
   * @param cacheLevel    Storage level to use for RDD caching
   * @param sink          function that utilize the result of the ingest, assumed to force materialization of the RDD
   * @tparam T            type of input tile key
   * @tparam K            type of output tile key, must have SpatialComponent
   * @return
   */
  def apply[T: ClassTag: ? => TilerKeyMethods[T, K]: Component[?, ProjectedExtent], K: SpatialComponent: Boundable: ClassTag](
      sourceTiles: DStream[(T, MultibandTile)],
      destCRS: CRS,
      layoutScheme: LayoutScheme,
      pyramid: Boolean = false,
      cacheLevel: StorageLevel = StorageLevel.NONE,
      resampleMethod: ResampleMethod = NearestNeighbor,
      partitioner: Option[Partitioner] = None,
      bufferSize: Option[Int] = None
    )
    (sink: (MultibandTileLayerRDD[K], Int) => Unit): Unit =
      sourceTiles
        .foreachRDD { rdd =>
          if(!rdd.isEmpty)
            RDDMultibandIngest[T, K](rdd, destCRS, layoutScheme, pyramid, cacheLevel, resampleMethod, partitioner, bufferSize)(sink)
        }
}