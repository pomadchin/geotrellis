package geotrellis.raster.io.geotiff.compression

import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.testkit._
import geotrellis.vector.Extent
import java.net.URL
import java.io.File
import java.util.concurrent.Executors

import org.scalatest._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Random
import sys.process._


class JPEGLoadGeoTiffReaderSpec extends FunSpec
    with RasterMatchers
    with BeforeAndAfterAll
    with GeoTiffTestUtils {

  override def afterAll = purge

  describe("Reading GeoTiffs with JPEG compression") {
    it("Does not cause Too many open files exception") {

      val url = geoTiffPath(s"jpeg-test-small.tif")

      val temp = File.createTempFile("oam-scene", ".tif")
      val jpegRasterPath = temp.getPath

      addToPurge(jpegRasterPath)

      println(f"Downloading ${url}...")

      new URL(url) #> new File(jpegRasterPath) !!

      println(f"Starting test...")

      val extent = RasterSource(jpegRasterPath).metadata.gridExtent.extent

      val pool = Executors.newFixedThreadPool(100)
      implicit val ec = ExecutionContext.fromExecutor(pool)

      val list = (1 to 10000).toList

      try {
        val result =
          Await.result(Future.sequence(list.map { _ =>
            Future {
              val (xmin, ymin) = (
                (Random.nextDouble * (extent.width - 1)) + extent.xmin,
                (Random.nextDouble * (extent.height - 1)) + extent.ymin
              )

              val windowExtent = Extent(
                xmin,
                ymin,
                xmin + 1,
                ymin + 1
              )

              RasterSource(jpegRasterPath).read(windowExtent).map { r =>
                // Do something to ensure the JVM doesn't optimize things away.
                val m = r._1.band(1).mutable
                m.set(0, 0, 1)
              }

              info("READ")
            }
          }), Duration.Inf)
      } finally pool.shutdown()

      println("DONE")
    }
  }
}
