package geotrellis.spark.summary.polygonal

import cats.Semigroup
import cats.syntax.semigroup._
import geotrellis.layer.{Metadata, SpatialKey, TileLayerMetadata}
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.raster.summary.GridVisitor
import geotrellis.raster.summary.polygonal._
import geotrellis.util.MethodExtensions
import geotrellis.vector._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

trait RDDPolygonalSummaryMethods[R, T <: CellGrid[Int]]
  extends MethodExtensions[RDD[(SpatialKey, T)] with Metadata[TileLayerMetadata[SpatialKey]]] {
  /**
    * @see [[RDDPolygonalSummary]]
    */
  def polygonalSummary(geometryRdd: RDD[Geometry],
                       visitor: GridVisitor[Raster[T], R],
                       options: Rasterizer.Options)(
                        implicit semigroupR: Semigroup[R], tagR: ClassTag[R]): RDD[Feature[Geometry, PolygonalSummaryResult[R]]] = {
    RDDPolygonalSummary(self, geometryRdd, visitor, options)
  }

  /**
    * Helper method that automatically lifts Seq[Geometry] into RDD[Geometry]
    *
    * @see [[RDDPolygonalSummary]]
    */
  def polygonalSummary(geometries: Seq[Geometry],
                       visitor: GridVisitor[Raster[T], R],
                       options: Rasterizer.Options)(
                        implicit semigroupR: Semigroup[R], tagR: ClassTag[R]): RDD[Feature[Geometry, PolygonalSummaryResult[R]]] = {
    self.polygonalSummary(self.sparkContext.parallelize(geometries), visitor, options)
  }

  /**
    * Helper method that automatically lifts Seq[Geometry] into RDD[Geometry]
    * and uses the default Rasterizer.Options
    *
    * @see [[RDDPolygonalSummary]]
    */
  def polygonalSummary(geometries: Seq[Geometry],
                       visitor: GridVisitor[Raster[T], R])(
                        implicit semigroupR: Semigroup[R], tagR: ClassTag[R]): RDD[Feature[Geometry, PolygonalSummaryResult[R]]] = {
    self.polygonalSummary(self.sparkContext.parallelize(geometries),
      visitor,
      PolygonalSummary.DefaultOptions)
  }

  /**
    * Helper method for performing a polygonal summary across a raster RDD
    * that takes only a single Geometry and returns a single PolygonalSummaryResult[R]
    * for that Geometry.
    *
    * @see [[RDDPolygonalSummary]]
    */
  def polygonalSummaryValue(geometry: Geometry,
                            visitor: GridVisitor[Raster[T], R],
                            options: Rasterizer.Options)(
                             implicit semigroupR: Semigroup[R], tagR: ClassTag[R]): PolygonalSummaryResult[R] = {
    self
      .polygonalSummary(self.sparkContext.parallelize(List(geometry)), visitor, options)
      .map { _.data }
      .reduce { _.combine(_) }
  }

  /**
    * Helper method for performing a polygonal summary across a raster RDD
    * that takes only a single Geometry and returns a single PolygonalSummaryResult[R]
    * for that Geometry. It uses the default Rasterizer.Options
    *
    * @see [[RDDPolygonalSummary]]
    */
  def polygonalSummaryValue(geometry: Geometry,
                            visitor: GridVisitor[Raster[T], R])(
                             implicit semigroupR: Semigroup[R], tagR: ClassTag[R]): PolygonalSummaryResult[R] = {
    self.polygonalSummaryValue(geometry, visitor, PolygonalSummary.DefaultOptions)
  }
}