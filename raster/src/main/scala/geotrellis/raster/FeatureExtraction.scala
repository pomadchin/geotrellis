package geotrellis.raster

import geotrellis.raster.rasterize.Rasterizer
import geotrellis.vector._
import spire.syntax.cfor._

import scala.collection.mutable.ListBuffer

trait FeatureExtraction[G <: Geometry, T <: CellGrid[Int], D] {
  def extract(geom: G, raster: Raster[T]): Feature[G, D]
}

object FeatureExtraction {
  implicit def multibandTile[G <: Geometry] = new FeatureExtraction[G, MultibandTile, Array[Array[Int]]] {
    def extract(geom: G, raster: Raster[MultibandTile]): Feature[G, Array[Array[Int]]] = {
      val arr = Array.ofDim[Array[Int]](raster.tile.bandCount)

      cfor(0)(_ < raster.tile.bandCount, _ + 1) { i =>
        val buffer = ListBuffer[Int]()
        Rasterizer.foreachCellByGeometry(geom, raster.rasterExtent) { case (col, row) => buffer += raster.tile.band(i).get(col, row) }
        arr(i) = buffer.toArray
      }
      Feature(geom, arr)
    }
  }

  implicit def multibandTileDouble[G <: Geometry] = new FeatureExtraction[G, MultibandTile, Array[Array[Double]]] {
    def extract(geom: G, raster: Raster[MultibandTile]): Feature[G, Array[Array[Double]]] = {
      val arr = Array.ofDim[Array[Double]](raster.tile.bandCount)

      cfor(0)(_ < raster.tile.bandCount, _ + 1) { i =>
        val buffer = ListBuffer[Double]()
        Rasterizer.foreachCellByGeometry(geom, raster.rasterExtent) { case (col, row) => buffer += raster.tile.band(i).getDouble(col, row) }
        arr(i) = buffer.toArray
      }
      Feature(geom, arr)
    }
  }

  implicit def tile[G <: Geometry] = new FeatureExtraction[G, Tile, Array[Int]] {
    def extract(geom: G, raster: Raster[Tile]): Feature[G, Array[Int]] =
      multibandTile[G].extract(geom, raster.mapTile(MultibandTile(_))).mapData(_.head)
  }

  implicit def tileDouble[G <: Geometry] = new FeatureExtraction[G, Tile, Array[Double]] {
    def extract(geom: G, raster: Raster[Tile]): Feature[G, Array[Double]] =
      multibandTileDouble[G].extract(geom, raster.mapTile(MultibandTile(_))).mapData(_.head)
  }
}
