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

package geotrellis.raster.rasterize

import geotrellis.raster._
import geotrellis.vector._
import geotrellis.raster.testkit._
import math.{max,min,round}

import org.scalatest._

import geotrellis.raster.testkit._
import scala.collection.mutable

class RasterizeSpec extends FunSuite with RasterMatchers
                                     with Matchers {
   test("Point Rasterization") {
      val e = Extent(0.0, 0.0, 10.0, 10.0)
      val re = RasterExtent(e, 1.0, 1.0, 10, 10)

      val data = (0 until 99).toArray
      val tile = ArrayTile(data, re.cols, re.rows)

      val p = PointFeature(Point(1.0,2.0), "point one: ")
      val p2 = PointFeature(Point(9.5, 9.5), "point two: ")
      val p3 = PointFeature(Point(0.1, 9.9), "point three: ")


      var f2output:String = ""

      Rasterizer.foreachCellByPoint(p.geom, re) { (col:Int, row:Int) =>
        val z = tile.get(col,row)
        f2output = f2output + p.data + z.toString
      }
      assert(f2output === "point one: 81")

      f2output = ""
      Rasterizer.foreachCellByPoint(p2.geom, re) { (col:Int, row:Int) =>
        val z = tile.get(col,row)
        f2output = f2output + p2.data + z.toString
      }
      assert( f2output === "point two: 9")

      f2output = ""
      Rasterizer.foreachCellByPoint(p3.geom, re) { (col:Int, row:Int) =>
        val z = tile.get(col,row)
        f2output = f2output + p3.data + z.toString
      }
      assert( f2output === "point three: 0")

      var lineOutput = ""
      val line = LineFeature(Line((0.0,0.0),(9.0,9.0)),"diagonal line")
      Rasterizer.foreachCellByLineString(line.geom, re) { (col:Int, row:Int) =>
        lineOutput = lineOutput + line.data + tile.get(col,row) + "\n"
      }
  }

  test("linestring rasterization") {
    // setup test objects
    val e = Extent(0.0, 0.0, 10.0, 10.0)
    val re = RasterExtent(e, 1.0, 1.0, 10, 10)

    val data = (0 until 99).toArray
    val tile = ArrayTile(data, re.cols, re.rows)

    val line1 = LineFeature(Line((1.0,3.5),(1.0,8.5)), "line" )
    var lineOutput:String = ""
    Rasterizer.foreachCellByLineString(line1.geom, re) { (col:Int, row:Int) =>
      lineOutput = lineOutput + tile.get(col,row) + ","
    }
    assert(lineOutput === "61,51,41,31,21,11,")
  }

  test("linestring rasterization with multiple points") {
    // setup test objects
    val e = Extent(0.0, 0.0, 10.0, 10.0)
    val re = RasterExtent(e, 1.0, 1.0, 10, 10)

    val data = (0 until 99).toArray
    val tile = ArrayTile(data, re.cols, re.rows)

    val line1 = LineFeature(Line((1.0,3.5),(1.0,8.5), (5.0, 9.0)), "line" )
    val result = mutable.ListBuffer[(Int,Int)]()

    Rasterizer.foreachCellByLineString(line1.geom, re) { (col:Int, row:Int) =>
      result += ((col,row))
    }

    result.toSeq should be (Seq( (1,6),
                                 (1,5),
                                 (1,4),
                                 (1,3),
                                 (1,2),
                                 (1,1),(2,1),(3,1),(4,1),(5,1)))
  }

  test("linestring rasterization with multiple points in diagonal") {
    // setup test objects
    val e = Extent(0.0, 0.0, 10.0, 10.0)
    val re = RasterExtent(e, 1.0, 1.0, 10, 10)

    val data = (0 until 99).toArray
    val tile = ArrayTile(data, re.cols, re.rows)

    val line1 = LineFeature(Line((1.0,3.5),(1.0,8.5), (5.0, 9.0),(1.0,4.5)), "line" )
    val result = mutable.ListBuffer[(Int,Int)]()

    Rasterizer.foreachCellByLineString(line1.geom, re) { (col:Int, row:Int) =>
      result += ((col,row))
    }
    result.toSeq should be (Seq( (1,6),
                                 (1,5),
                                 (1,4),
                                 (1,3),
                                 (1,2),
                                 (1,1),(2,1),(3,1),(4,1),(5,1),
                                                   (4,2),
                                             (3,3),
                                       (2,4),
                                  (1,5)))
  }

  test("4-connecting line drawing 1") {
    val e = Extent(0, 0, 1024, 1024)
    val re = RasterExtent(e, 1024, 1024)
    val line = Line((0, 0), (1022, 1024))
    val result4 = mutable.Set[(Int, Int)]()
    val result8 = mutable.Set[(Int, Int)]()

    Rasterizer.foreachCellByLineString(line, re, FourNeighbors)({ (col: Int, row: Int) =>
      if (col == 512) result4 += ((col, row)) })
    Rasterizer.foreachCellByLineString(line, re)({ (col: Int, row: Int) =>
      if (col == 512) result8 += ((col, row)) })

    (result4 diff result8) should be (Set((512,512)))
    (result8 diff result4) should be (Set.empty)
  }

  test("4-connecting line drawing 2") {
    val e = Extent(0, 0, 1024, 1024)
    val re = RasterExtent(e, 1024, 1024)
    val line = Line((0, 0), (1024, 1022))
    val result4 = mutable.Set[(Int, Int)]()
    val result8 = mutable.Set[(Int, Int)]()

    Rasterizer.foreachCellByLineString(line, re, FourNeighbors)({ (col: Int, row: Int) =>
      if (col == 512) result4 += ((col, row)) })
    Rasterizer.foreachCellByLineString(line, re)({ (col: Int, row: Int) =>
      if (col == 512) result8 += ((col, row)) })

    (result4 diff result8) should be (Set((512,514)))
    (result8 diff result4) should be (Set.empty)
  }

}
