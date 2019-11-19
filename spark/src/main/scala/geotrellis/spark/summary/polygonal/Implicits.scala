package geotrellis.spark.summary.polygonal

import geotrellis.layer.{Metadata, SpatialKey, TileLayerMetadata}
import geotrellis.raster.CellGrid
import geotrellis.spark.summary.polygonal.RDDPolygonalSummary.RDDPolygonalSummaryMethods
import org.apache.spark.rdd.RDD

trait Implicits {
  implicit class withRDDPolygonalSummaryMethods[R, T <: CellGrid[Int]](val self: RDD[(SpatialKey, T)] with Metadata[TileLayerMetadata[SpatialKey]])
    extends RDDPolygonalSummaryMethods[R, T]
}

object Implicits extends Implicits