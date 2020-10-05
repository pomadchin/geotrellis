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

package geotrellis.vector.voronoi

import geotrellis.util.MethodExtensions
import geotrellis.vector._
import org.locationtech.jts.geom.Coordinate

trait VoronoiDiagramCoordinateMethods extends MethodExtensions[Traversable[Coordinate]] {
  def voronoiDiagram(extent: Extent): VoronoiDiagram = { VoronoiDiagram(self.toArray, extent) }
}

trait VoronoiDiagramPointMethods extends MethodExtensions[Traversable[Point]] {
  def voronoiDiagram(extent: Extent): VoronoiDiagram = { VoronoiDiagram(self.map(_.getCoordinate).toArray, extent) }
}

trait VoronoiDiagramCoordinateArrayMethods extends MethodExtensions[Array[Coordinate]] {
  def voronoiDiagram(extent: Extent): VoronoiDiagram = { VoronoiDiagram(self, extent) }
}

trait VoronoiDiagramPointArrayMethods extends MethodExtensions[Array[Point]] {
  def voronoiDiagram(extent: Extent): VoronoiDiagram = { VoronoiDiagram(self.map(_.getCoordinate), extent) }
}

trait VoronoiDiagramMultiPointMethods extends MethodExtensions[MultiPoint] {
  def voronoiDiagram(extent: Extent): VoronoiDiagram = { VoronoiDiagram(self.points.map(_.getCoordinate), extent) }
}
