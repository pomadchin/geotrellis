package geotrellis.vector.triangulation

import java.nio.charset.Charset

import com.vividsolutions.jts.geom.Coordinate
import spire.syntax.cfor._
import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.rasterize._
import geotrellis.raster.render._
import geotrellis.spark.io.s3.S3Client

import scala.util.Random
import org.scalatest.{FunSpec, Matchers}

import scala.collection.IterableLike
import scala.collection.generic.CanBuildFrom

class DelaunayTriangulationDecimationSpec extends FunSpec with Matchers {

  class RichCollection[A, Repr](xs: IterableLike[A, Repr]){
    def distinctBy[B, That](f: A => B)(implicit cbf: CanBuildFrom[Repr, A, That]) = {
      val builder = cbf(xs.repr)
      val i = xs.iterator
      var set = Set[B]()
      while (i.hasNext) {
        val o = i.next
        val b = f(o)
        if (!set(b)) {
          set += b
          builder += o
        }
      }
      builder.result
    }
  }

  implicit def toRich[A, Repr](xs: IterableLike[A, Repr]): RichCollection[A, Repr] = new RichCollection(xs)

  println("Starting tests for DelaunayTriangulationDecimationSpec")

  val numpts = 2000

  def randInRange(low: Double, high: Double): Double = {
    val x = Random.nextDouble
    low * (1-x) + high * x
  }

  def randomPoint(extent: Extent): Coordinate = {
    new Coordinate(randInRange(extent.xmin, extent.xmax), randInRange(extent.ymin, extent.ymax))
  }

  def randomizedGrid(n: Int, extent: Extent): Seq[Coordinate] = {
    val xs = (for (i <- 1 to n) yield randInRange(extent.xmin, extent.xmax)).sorted
    val ys = for (i <- 1 to n*n) yield randInRange(extent.ymin, extent.ymax)

    xs.flatMap{ x => {
      val yvals = Random.shuffle(ys).take(n).sorted
      yvals.map{ y => new Coordinate(x, y) }
    }}
  }

  lazy val s3Client = S3Client.DEFAULT

  describe("Delaunay Triangulation Decimation") {
    it("should triangulate and decimate [NOTE: should be moved into DelanayTriangulationSpec as probable data race was noticed]") {

      val is = s3Client.getObject("geotrellis-test", "decimation-debug/pts194303.txt").getObjectContent

      val pts = scala.io.Source.fromInputStream(is)(Charset.forName("UTF-8")).getLines().toList.map { l =>
        val Array(x, y, z) = l.split(", ").map { d =>
          d.toDouble
          // BigDecimal(d.toDouble).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
        }
        new Coordinate(x, y, z)
      }

      val ptsd = pts.distinctBy { c => (c.x, c.y, c.z) }

      println(s"${pts.length} == ${ptsd.length}: ${pts.length == ptsd.length}")

      val length = ptsd.length
      val by = (0.75 * length).toInt

      val dt = DelaunayTriangulation(ptsd.toArray)

      println(dt.halfEdgeTable.allVertices().find(_ == 9410))

      dt.decimate(by)
    }
  }

}
