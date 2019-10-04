/*
 * Copyright 2019 Azavea
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

package geotrellis.raster

import geotrellis.vector.Point
import geotrellis.macros.{NoDataMacros, TypeConversionMacros}
import geotrellis.vector._
import geotrellis.util.{MethodExtensions, np}


object Implicits extends Implicits

trait Implicits
    extends costdistance.Implicits
    with crop.Implicits
    with density.Implicits
    with distance.Implicits
    with equalization.Implicits
    with hydrology.Implicits
    with interpolation.Implicits
    with io.json.Implicits
    with mapalgebra.focal.Implicits
    with mapalgebra.focal.hillshade.Implicits
    with mapalgebra.local.Implicits
    with mapalgebra.zonal.Implicits
    with mask.Implicits
    with matching.Implicits
    with merge.Implicits
    with prototype.Implicits
    with rasterize.Implicits
    with regiongroup.Implicits
    with render.Implicits
    with reproject.Implicits
    with resample.Implicits
    with sigmoidal.Implicits
    with split.Implicits
    with summary.Implicits
    with summary.polygonal.Implicits
    with transform.Implicits
    with vectorize.Implicits
    with viewshed.Implicits {

  // Implicit method extension for core types

  implicit class withTileMethods(val self: Tile) extends MethodExtensions[Tile]
      with DelayedConversionTileMethods

  implicit class withMultibandTileMethods(val self: MultibandTile) extends MethodExtensions[MultibandTile]
      with DelayedConversionMultibandTileMethods

  implicit class SinglebandRasterAnyRefMethods(val self: SinglebandRaster) extends AnyRef {
    def getValueAtPoint(point: Point): Int =
      getValueAtPoint(point.x, point.y)

    def getValueAtPoint(x: Double, y: Double): Int =
      self.tile.get(
        self.rasterExtent.mapXToGrid(x),
        self.rasterExtent.mapYToGrid(y)
      )

    def getDoubleValueAtPoint(point: Point): Double =
      getDoubleValueAtPoint(point.x, point.y)

    def getDoubleValueAtPoint(x: Double, y: Double): Double =
      self.tile.getDouble(
        self.rasterExtent.mapXToGrid(x),
        self.rasterExtent.mapYToGrid(y)
      )
  }

  implicit class TraversableTileExtensions(rs: Traversable[Tile]) {
    def assertEqualDimensions(): Unit =
      if(Set(rs.map(_.dimensions)).size != 1) {
        val dimensions = rs.map(_.dimensions).toSeq
        throw new GeoAttrsError("Cannot combine tiles with different dimensions." +
          s"$dimensions are not all equal")
      }
  }

  implicit class TileTupleExtensions(t: (Tile, Tile)) {
    def assertEqualDimensions(): Unit =
      if(t._1.dimensions != t._2.dimensions) {
        throw new GeoAttrsError("Cannot combine rasters with different dimensions." +
          s"${t._1.dimensions} does not match ${t._2.dimensions}")
      }
  }

  implicit class TilePercentileExtensions(tile: Tile) {
    /**
      * Compute percentile at the given breaks using the same algorithm as numpy
      *
      * https://docs.scipy.org/doc/numpy/reference/generated/numpy.percentile.html
      * https://en.wikipedia.org/wiki/Percentile
      *
      * @param pctBreaks
      * @return
      */
    def percentile(pctBreaks: Array[Double]): Array[Double] = {
      np.percentile(tile.toArrayDouble.filter(isData(_)), pctBreaks)
    }

    /**
      * Compute percentile at the given break using the same algorithm as numpy
      *
      * https://docs.scipy.org/doc/numpy/reference/generated/numpy.percentile.html
      * https://en.wikipedia.org/wiki/Percentile
      *
      * @param pctBreak
      * @return
      */
    def percentile(pctBreak: Double): Double = {
      np.percentile(tile.toArrayDouble.filter(isData(_)), pctBreak)
    }
  }

  implicit class MultibandRasterFeatureExtractionMethods(val self: Raster[MultibandTile]) extends MethodExtensions[Raster[MultibandTile]] {
    def extract[D, G <: Geometry: FeatureExtraction[*, MultibandTile, Array[Array[D]]]](geom: G, raster: Raster[MultibandTile]): Feature[G, Array[Array[D]]] =
      implicitly[FeatureExtraction[G, MultibandTile, Array[Array[D]]]].extract(geom, raster)

    def extractInt[G <: Geometry: FeatureExtraction[*, MultibandTile, Array[Array[Int]]]](geom: G, raster: Raster[MultibandTile]): Feature[G, Array[Array[Int]]] =
      extract[Int, G](geom, raster)

    def extractDouble[G <: Geometry: FeatureExtraction[*, MultibandTile, Array[Array[Double]]]](geom: G, raster: Raster[MultibandTile]): Feature[G, Array[Array[Double]]] =
      extract[Double, G](geom, raster)
  }

  implicit class rasterFeatureExtractionMethods(val self: Raster[Tile]) extends MethodExtensions[Raster[Tile]] {
    def extract[D, G <: Geometry: FeatureExtraction[*, Tile, Array[D]]](geom: G, raster: Raster[Tile]): Feature[G, Array[D]] =
      implicitly[FeatureExtraction[G, Tile, Array[D]]].extract(geom, raster)

    def extractInt[G <: Geometry: FeatureExtraction[*, Tile, Array[Int]]](geom: G, raster: Raster[Tile]): Feature[G, Array[Int]] =
      extract[Int, G](geom, raster)

    def extractDouble[G <: Geometry: FeatureExtraction[*, Tile, Array[Double]]](geom: G, raster: Raster[Tile]): Feature[G, Array[Double]] =
      extract[Double, G](geom, raster)
  }

  implicit class MultibandRasterFeatureExtraction2Methods(val self: Raster[MultibandTile]) extends MethodExtensions[Raster[MultibandTile]] {
    def extract[D, G <: Geometry: FeatureExtraction2[*, MultibandTile, D]](geom: G, raster: Raster[MultibandTile]): Array[Array[PointFeature[D]]] =
      implicitly[FeatureExtraction2[G, MultibandTile, D]].extract(geom, raster)

    def extractInt[G <: Geometry: FeatureExtraction2[*, MultibandTile, Int]](geom: G, raster: Raster[MultibandTile]): Array[Array[PointFeature[Int]]] =
      extract[Int, G](geom, raster)

    def extractDouble[G <: Geometry: FeatureExtraction2[*, MultibandTile, Double]](geom: G, raster: Raster[MultibandTile]): Array[Array[PointFeature[Double]]] =
      extract[Double, G](geom, raster)
  }

  implicit class rasterFeatureExtraction2Methods(val self: Raster[Tile]) extends MethodExtensions[Raster[Tile]] {
    def extract[D, G <: Geometry: FeatureExtraction2[*, Tile, D]](geom: G, raster: Raster[Tile]): Array[PointFeature[D]] =
      implicitly[FeatureExtraction2[G, Tile, D]].extract(geom, raster).head

    def extractInt[G <: Geometry: FeatureExtraction2[*, Tile, Int]](geom: G, raster: Raster[Tile]): Array[PointFeature[Int]] =
      extract[Int, G](geom, raster)

    def extractDouble[G <: Geometry: FeatureExtraction2[*, Tile, Double]](geom: G, raster: Raster[Tile]): Array[PointFeature[Double]] =
      extract[Double, G](geom, raster)
  }
}
