package geotrellis.raster

import geotrellis.raster.rasterize.Rasterizer
import geotrellis.vector.{Feature, Geometry, Point, PointFeature}
import spire.syntax.cfor._

import scala.collection.mutable.ListBuffer

trait FeatureExtraction2[G <: Geometry, T <: CellGrid[Int], D] {
  def extract(geom: G, raster: Raster[T]): Array[Array[PointFeature[D]]]
}

object FeatureExtraction2 {
  implicit def multibandTile[G <: Geometry] = new FeatureExtraction2[G, MultibandTile, Int] {
    def extract(geom: G, raster: Raster[MultibandTile]): Array[Array[PointFeature[Int]]] = {
      val arr = Array.ofDim[Array[PointFeature[Int]]](raster.tile.bandCount)

      cfor(0)(_ < raster.tile.bandCount, _ + 1) { i =>
        val buffer = ListBuffer[PointFeature[Int]]()
        Rasterizer.foreachCellByGeometry(geom, raster.rasterExtent) { case (col, row) =>
          buffer += Feature(Point(raster.rasterExtent.gridToMap(col, row)), raster.tile.band(i).get(col, row))
        }
        arr(i) = buffer.toArray
      }

      arr
    }
  }

  implicit def multibandTileDouble[G <: Geometry] = new FeatureExtraction2[G, MultibandTile, Double] {
    def extract(geom: G, raster: Raster[MultibandTile]): Array[Array[PointFeature[Double]]] = {
      val arr = Array.ofDim[Array[PointFeature[Double]]](raster.tile.bandCount)

      cfor(0)(_ < raster.tile.bandCount, _ + 1) { i =>
        val buffer = ListBuffer[PointFeature[Double]]()
        Rasterizer.foreachCellByGeometry(geom, raster.rasterExtent) { case (col, row) =>
          buffer += Feature(Point(raster.rasterExtent.gridToMap(col, row)), raster.tile.band(i).getDouble(col, row))
        }
        arr(i) = buffer.toArray
      }

      arr
    }
  }

  implicit def tile[G <: Geometry] = new FeatureExtraction2[G, Tile, Int] {
    def extract(geom: G, raster: Raster[Tile]): Array[Array[PointFeature[Int]]] = {
      multibandTile[G].extract(geom, raster.mapTile(MultibandTile(_)))
    }
  }

  implicit def tileDouble[G <: Geometry] = new FeatureExtraction2[G, Tile, Double] {
    def extract(geom: G, raster: Raster[Tile]): Array[Array[PointFeature[Double]]] = {
      multibandTileDouble[G].extract(geom, raster.mapTile(MultibandTile(_)))
    }
  }
}
