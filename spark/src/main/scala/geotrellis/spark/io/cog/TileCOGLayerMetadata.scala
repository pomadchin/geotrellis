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

package geotrellis.spark.io.cog

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.spark.ingest._
import geotrellis.vector.{ProjectedExtent, Extent}
import geotrellis.util._

import geotrellis.proj4.CRS

import org.apache.spark.rdd._

/**
 * @param cellType    value type of each cell
 * @param layout      definition of the tiled raster layout
 * @param extent      Extent covering the source data
 * @param crs         CRS of the raster projection
 */

/*case class TileCOGLayerMetadata[K](
  cellType: CellType,
  layout: LayoutDefinition,
  extent: Extent,
  crs: CRS,
  bounds: Bounds[K]
) {
  /** Transformations between tiling scheme and map references */
  def mapTransform = layout.mapTransform
  /** TileLayout of the layout */
  def tileLayout = layout.tileLayout
  /** Full extent of the layout */
  def layoutExtent = layout.extent
  /** GridBounds of data tiles in the layout */
  def gridBounds = mapTransform(extent)

  def combine(other: TileCOGLayerMetadata[K])(implicit b: Boundable[K]): TileCOGLayerMetadata[K] = {
    val combinedExtent       = extent combine other.extent
    val combinedLayoutExtent = layout.extent combine other.layout.extent
    val combinedTileLayout   = layout.tileLayout combine other.layout.tileLayout
    val combinedBounds       = bounds combine other.bounds

    this
      .copy(
        extent = combinedExtent,
        bounds = combinedBounds,
        layout = this.layout
          .copy(
            extent     = combinedLayoutExtent,
            tileLayout = combinedTileLayout
          )
      )
  }

  def updateBounds(newBounds: Bounds[K])(implicit c: Component[K, SpatialKey]): TileCOGLayerMetadata[K] =
    newBounds match {
      case kb: KeyBounds[K] => {
        val SpatialKey(minCol, minRow) = kb.minKey.getComponent[SpatialKey]
        val SpatialKey(maxCol, maxRow) = kb.maxKey.getComponent[SpatialKey]
        val kbExtent = mapTransform(GridBounds(minCol, minRow, maxCol, maxRow))

        kbExtent.intersection(extent) match {
          case Some(e) =>
            copy(bounds = newBounds, extent = e)
          case None =>
            copy(bounds = newBounds, extent = Extent(extent.xmin, extent.ymin, extent.xmin, extent.ymin))
        }
      }
      case EmptyBounds =>
        copy(bounds = newBounds, extent = Extent(extent.xmin, extent.ymin, extent.xmin, extent.ymin))
    }
}

object TileCOGLayerMetadata {
  implicit def toLayoutDefinition(md: TileCOGLayerMetadata[_]): LayoutDefinition =
    md.layout

  implicit def extentComponent[K]: GetComponent[TileCOGLayerMetadata[K], Extent] =
    GetComponent(_.extent)

  implicit def crsComponent[K]: GetComponent[TileCOGLayerMetadata[K], CRS] =
    GetComponent(_.crs)

  implicit def layoutComponent[K: SpatialComponent]: Component[TileCOGLayerMetadata[K], LayoutDefinition] =
    Component(_.layout, (md, l) => md.copy(layout = l))

  implicit def boundsComponent[K: SpatialComponent]: Component[TileCOGLayerMetadata[K], Bounds[K]] =
    Component(_.bounds, (md, b) => md.updateBounds(b))

  implicit def mergable[K: Boundable]: merge.Mergable[TileCOGLayerMetadata[K]] =
    new merge.Mergable[TileCOGLayerMetadata[K]] {
      def merge(t1: TileCOGLayerMetadata[K], t2: TileCOGLayerMetadata[K]): TileCOGLayerMetadata[K] =
        t1.combine(t2)
    }

  private def collectMetadata[
    K: (? => TilerKeyMethods[K, K2]),
    V <: CellGrid,
    K2: SpatialComponent: Boundable
  ](rdd: RDD[(K, V)]): (Extent, CellType, CellSize, KeyBounds[K2]) = {
    rdd
      .map { case (key, grid) =>
        val extent = key.extent
        val boundsKey = key.translate(SpatialKey(0,0))
        // Bounds are return to set the non-spatial dimensions of the keybounds; the spatial keybounds are set outside this call.
        (extent, grid.cellType, CellSize(extent, grid.cols, grid.rows), KeyBounds(boundsKey, boundsKey))
      }
      .reduce { (tuple1, tuple2) =>
        val (extent1, cellType1, cellSize1, bounds1) = tuple1
        val (extent2, cellType2, cellSize2, bounds2) = tuple2
        (
          extent1.combine(extent2),
          cellType1.union(cellType2),
          if (cellSize1.resolution < cellSize2.resolution) cellSize1 else cellSize2,
          bounds1.combine(bounds2)
        )
      }
  }

  private def collectMetadataWithCRS[
    K: GetComponent[?, ProjectedExtent]: (? => TilerKeyMethods[K, K2]),
    V <: CellGrid,
    K2: SpatialComponent: Boundable
  ](rdd: RDD[(K, V)]): (Extent, CellType, CellSize, KeyBounds[K2], CRS) = {
    val (extent, cellType, cellSize, crsSet, bounds) =
      rdd
      .map { case (key, grid) =>
        val ProjectedExtent(extent, crs) = key.getComponent[ProjectedExtent]
        val boundsKey = key.translate(SpatialKey(0,0))
        // Bounds are return to set the non-spatial dimensions of the keybounds; the spatial keybounds are set outside this call.
        (extent, grid.cellType, CellSize(extent, grid.cols, grid.rows), Set(crs), KeyBounds(boundsKey, boundsKey))
      }
      .reduce { (tuple1, tuple2) =>
        val (extent1, cellType1, cellSize1, crs1, bounds1) = tuple1
        val (extent2, cellType2, cellSize2, crs2, bounds2) = tuple2
        (
          extent1.combine(extent2),
          cellType1.union(cellType2),
          if (cellSize1.resolution < cellSize2.resolution) cellSize1 else cellSize2,
          crs1 ++ crs2,
          bounds1.combine(bounds2)
          )
      }
    require(crsSet.size == 1, s"Multiple CRS tags found: $crsSet")
    (extent, cellType, cellSize, bounds, crsSet.head)
  }

  /**
    * Compose Extents from given raster tiles and fit it on given
    * TileLayout.
    */
  def fromRdd[
    K: (? => TilerKeyMethods[K, K2]),
    V <: CellGrid,
    K2: SpatialComponent: Boundable
  ](rdd: RDD[(K, V)], crs: CRS, layout: LayoutDefinition): TileCOGLayerMetadata[K2] = {
    val (extent, cellType, _, bounds) = collectMetadata(rdd)
    val kb = bounds.setSpatialBounds(KeyBounds(layout.mapTransform(extent)))
    TileCOGLayerMetadata(cellType, layout, extent, crs, kb)
  }

  /**
    * Compose Extents from given raster tiles and use LayoutScheme to
    * create the LayoutDefinition.
    */
  def fromRdd[
    K: (? => TilerKeyMethods[K, K2]) ,
    V <: CellGrid,
    K2: SpatialComponent: Boundable
  ](rdd: RDD[(K, V)], crs: CRS, scheme: LayoutScheme): (Int, TileCOGLayerMetadata[K2]) = {
    val (extent, cellType, cellSize, bounds) = collectMetadata(rdd)
    val LayoutLevel(zoom, layout) = scheme.levelFor(extent, cellSize)
    val kb = bounds.setSpatialBounds(KeyBounds(layout.mapTransform(extent)))
    (zoom, TileCOGLayerMetadata(cellType, layout, extent, crs, kb))
  }

  /**
    * Compose Extents from given raster tiles and use
    * [[geotrellis.spark.tiling.ZoomedLayoutScheme]] to create the
    * [[geotrellis.spark.tiling.LayoutDefinition]].
    */
  def fromRdd[
    K: (? => TilerKeyMethods[K, K2]) ,
    V <: CellGrid,
    K2: SpatialComponent: Boundable
  ](rdd: RDD[(K, V)], crs: CRS, scheme: ZoomedLayoutScheme):
    (Int, TileCOGLayerMetadata[K2]) =
      _fromRdd[K, V, K2](rdd, crs, scheme, None)

  /**
    * Compose Extents from given raster tiles using
    * [[geotrellis.spark.tiling.ZoomedLayoutScheme]] and a maximum
    * zoom value.
    */
  def fromRdd[
    K: (? => TilerKeyMethods[K, K2]) ,
    V <: CellGrid,
    K2: SpatialComponent: Boundable
  ](rdd: RDD[(K, V)], crs: CRS, scheme: ZoomedLayoutScheme, maxZoom: Int):
    (Int, TileCOGLayerMetadata[K2]) =
      _fromRdd[K, V, K2](rdd, crs, scheme, Some(maxZoom))

  private def _fromRdd[
    K: (? => TilerKeyMethods[K, K2]) ,
    V <: CellGrid,
    K2: SpatialComponent: Boundable
  ](rdd: RDD[(K, V)], crs: CRS, scheme: ZoomedLayoutScheme, maxZoom: Option[Int]):
    (Int, TileCOGLayerMetadata[K2]) = {
      val (extent, cellType, cellSize, bounds) = collectMetadata(rdd)
      val LayoutLevel(zoom, layout) = maxZoom match {
        case Some(zoom) => scheme.levelForZoom(maxZoom.get)
        case _ => scheme.levelFor(extent, cellSize)
      }
    val kb = bounds.setSpatialBounds(KeyBounds(layout.mapTransform(extent)))
    (zoom, TileCOGLayerMetadata(cellType, layout, extent, crs, kb))
  }

  def fromRdd[
    K: GetComponent[?, ProjectedExtent]: (? => TilerKeyMethods[K, K2]),
    V <: CellGrid,
    K2: SpatialComponent: Boundable
  ](rdd: RDD[(K, V)], scheme: LayoutScheme): (Int, TileCOGLayerMetadata[K2]) = {
    val (extent, cellType, cellSize, bounds, crs) = collectMetadataWithCRS(rdd)
    val LayoutLevel(zoom, layout) = scheme.levelFor(extent, cellSize)
    val GridBounds(colMin, rowMin, colMax, rowMax) = layout.mapTransform(extent)
    val kb = bounds.setSpatialBounds(KeyBounds(layout.mapTransform(extent)))
    (zoom, TileCOGLayerMetadata(cellType, layout, extent, crs, kb))
  }

  def fromRdd[
    K: GetComponent[?, ProjectedExtent]: (? => TilerKeyMethods[K, K2]),
    V <: CellGrid,
    K2: SpatialComponent: Boundable
  ](rdd: RDD[(K, V)],  scheme: ZoomedLayoutScheme):
    (Int, TileCOGLayerMetadata[K2]) =
      _fromRdd[K, V, K2](rdd, scheme, None)

  def fromRdd[
    K: GetComponent[?, ProjectedExtent]: (? => TilerKeyMethods[K, K2]),
    V <: CellGrid,
    K2: SpatialComponent: Boundable
  ](rdd: RDD[(K, V)], scheme: ZoomedLayoutScheme, maxZoom: Int):
    (Int, TileCOGLayerMetadata[K2]) =
      _fromRdd[K, V, K2](rdd, scheme, Some(maxZoom))

  private def _fromRdd[
    K: GetComponent[?, ProjectedExtent]: (? => TilerKeyMethods[K, K2]),
    V <: CellGrid,
    K2: SpatialComponent: Boundable
  ](rdd: RDD[(K, V)], scheme: ZoomedLayoutScheme, maxZoom: Option[Int]):
  (Int, TileCOGLayerMetadata[K2]) = {
    val (extent, cellType, cellSize, bounds, crs) = collectMetadataWithCRS(rdd)
    val LayoutLevel(zoom, layout) = maxZoom match {
      case Some(zoom) => scheme.levelForZoom(maxZoom.get)
      case _ => scheme.levelFor(extent, cellSize)
    }
    val GridBounds(colMin, rowMin, colMax, rowMax) = layout.mapTransform(extent)
    val kb = bounds.setSpatialBounds(KeyBounds(layout.mapTransform(extent)))
    (zoom, TileCOGLayerMetadata(cellType, layout, extent, crs, kb))
  }

  def fromRdd[
    K: GetComponent[?, ProjectedExtent]: (? => TilerKeyMethods[K, K2]),
    V <: CellGrid,
    K2: SpatialComponent: Boundable
  ](rdd: RDD[(K, V)], layoutDefinition: LayoutDefinition): TileCOGLayerMetadata[K2] = {
    val (extent, cellType, cellSize, bounds, crs) = collectMetadataWithCRS(rdd)
    val kb = bounds.setSpatialBounds(KeyBounds(layoutDefinition.mapTransform(extent)))
    TileCOGLayerMetadata(cellType, layoutDefinition, extent, crs, kb)
  }
}*/

