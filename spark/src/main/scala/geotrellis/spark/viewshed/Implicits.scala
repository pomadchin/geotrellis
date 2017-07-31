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

import geotrellis.spark._
import geotrellis.raster.Tile

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


object Implicits extends Implicits

trait Implicits {
  implicit class withRDDViewshedMethods[K: (? => SpatialKey): ClassTag, V: (? => Tile)](val self: RDD[(K, V)] with Metadata[TileLayerMetadata[K]])
      extends RDDViewshedMethods[K, V]
}
