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

package geotrellis.spark.viewshed

import geotrellis.raster.{Tile, DoubleArrayTile}
import geotrellis.raster.viewshed.R2Viewshed._
import geotrellis.spark._
import geotrellis.spark.viewshed.IterativeViewshed.Point6D
import geotrellis.util.MethodExtensions

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import scala.collection.mutable
import scala.reflect.ClassTag


abstract class RDDViewshedMethods[K: (? => SpatialKey): ClassTag, V: (? => Tile)]
    extends MethodExtensions[RDD[(K, V)] with Metadata[TileLayerMetadata[K]]] {

  def viewshed(
    points: Seq[Point6D],
    maxDistance: Double = Double.PositiveInfinity,
    curvature: Boolean = true,
    operator: AggregationOperator = Or
  )(implicit sc: SparkContext): RDD[(K, Tile)] with Metadata[TileLayerMetadata[K]] =
    IterativeViewshed(
      elevation = self,
      ps = points,
      maxDistance = maxDistance,
      curvature = curvature,
      operator = operator
    )

}
