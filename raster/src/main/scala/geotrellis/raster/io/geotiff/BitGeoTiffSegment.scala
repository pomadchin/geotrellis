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

package geotrellis.raster.io.geotiff

import geotrellis.raster._
import geotrellis.raster.io.geotiff.util._

import spire.syntax.cfor._

class BitGeoTiffSegment(val bytes: Array[Byte], cols: Int, rows: Int) extends GeoTiffSegment {
  // Notice the inversing of the byte; this is because the endian-ness
  // of the byte is ignored in bit rasters, so is flipped when we read it in
  // incorrectly.

  val size = cols * rows

  private val paddedCols = {
    // if (bytes.length % 8 == 0) cols
    // else {
      val bytesWidth = (cols + 7) / 8
      bytesWidth * 8
    // }
  }

  def getInt(i: Int): Int = get(i).toInt
  override def paddedGetInt(i: Int): Int = pget(i).toInt
  def getDouble(i: Int): Double = get(i).toDouble

  private def pindex(i: Int): Int = {
    val col = i % paddedCols
    val row = i / paddedCols

    val res = (row * paddedCols) + col
    /*println(s"iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")
    println(s"cols: ${cols}")
    println(s"rows: ${rows}")
    println(s"col: $i % cols: $col")
    println(s"row: $i / cols: $row")
    println(s"index: ($row * $paddedCols) + $col: ${res}")
    println(s"iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")*/
    // println(s"col: $i % $cols: $col")
    // println(s"row: $i / $cols: $row")
    // println(s"col: $i % $paddedCols: $col")
    // println(s"row: $i / $paddedCols: $row")
    // println(s"index: ($row * $paddedCols) + $col: ${res}")
    res
  }

  /** Creates a corrected index into the byte array that accounts for byte padding on rows */
  private def index(i: Int): Int = {
    val col = i % cols
    val row = i / cols

    val res = (row * paddedCols) + col
    /*println(s"iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")
    println(s"cols: ${cols}")
    println(s"rows: ${rows}")
    println(s"col: $i % cols: $col")
    println(s"row: $i / cols: $row")
    println(s"index: ($row * $paddedCols) + $col: ${res}")
    println(s"iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")*/
    // println(s"col: $i % $cols: $col")
    // println(s"row: $i / $cols: $row")
    // println(s"col: $i % $paddedCols: $col")
    // println(s"row: $i / $paddedCols: $row")
    // println(s"index: ($row * $paddedCols) + $col: ${res}")
    res
  }

  /**
    * **********************
    * index(904): 904
    * index(904) >> 3: 113
    * bytes(index >> 3): -20
    * getInt(i): 1
    * index(905): 905
    * index(905) >> 3: 113
    * bytes(index >> 3): -20
    * getInt(i): 1
    * index(906): 906
    * index(906) >> 3: 113
    * bytes(index >> 3): -20
    * getInt(i): 1
    * index(907): 907
    * index(907) >> 3: 113
    * bytes(index >> 3): -20
    * getInt(i): 0
    * index(908): 908
    * index(908) >> 3: 113
    * bytes(index >> 3): -20
    * getInt(i): 1
    * index(909): 909
    * index(909) >> 3: 113
    * bytes(index >> 3): -20
    * getInt(i): 1
    * index(910): 912
    * index(910) >> 3: 114
    * bytes(index >> 3): -33
    * getInt(i): 1
    * index(911): 913
    * index(911) >> 3: 114
    * bytes(index >> 3): -33
    * getInt(i): 1
    * **********************
    *
    * @param i
    * @return
    */
  def get(i: Int): Byte = {
    /*if(((index(i) >> 3) >= 110) && ((index(i) >> 3) <= 115) /*|| (index(i) >> 3) >= 340*/) {
      println
      println(s"index($i): ${index(i)}")
      println(s"index($i) >> 3: ${index(i) >> 3}")
      println(s"bytes(index >> 3): ${bytes(index(i) >> 3)}")
      println
    }*/

    /*println
    println(s"index($i): ${index(i)}")
    println(s"index($i) >> 3: ${index(i) >> 3}")
    println(s"bytes(index >> 3): ${bytes(index(i) >> 3)}")
    println*/

    val i2 = index(i)



    // println(s"index >> 3 : ${i2 >> 3}")
    ((invertByte(bytes(i2 >> 3)) >> (i2 & 7)) & 1).asInstanceOf[Byte]
  }

  def pget(i: Int): Byte = {
    /*if(((index(i) >> 3) >= 110) && ((index(i) >> 3) <= 115) /*|| (index(i) >> 3) >= 340*/) {
      println
      println(s"index($i): ${index(i)}")
      println(s"index($i) >> 3: ${index(i) >> 3}")
      println(s"bytes(index >> 3): ${bytes(index(i) >> 3)}")
      println
    }*/

    /*println
    println(s"index($i): ${index(i)}")
    println(s"index($i) >> 3: ${index(i) >> 3}")
    println(s"bytes(index >> 3): ${bytes(index(i) >> 3)}")
    println*/

    val i2 = pindex(i)



    // println(s"index >> 3 : ${i2 >> 3}")
    ((invertByte(bytes(i2 >> 3)) >> (i2 & 7)) & 1).asInstanceOf[Byte]
  }

  def map(f: Int => Int): Array[Byte] = {
    val arr = bytes.clone
    val f0 = f(0) & 1
    val f1 = f(1) & 1

    if (f0 == 0 && f1 == 0) {
      cfor(0)(_ < size, _ + 1) { i => arr(i) = 0.toByte }
    } else if (f0 == 1 && f1 == 1) {
      cfor(0)(_ < size, _ + 1) { i => arr(i) = -1.toByte }
    } else if (f0 != 0 || f1 != 1) {
      // inverse (complement) of what we have now
      var i = 0
      val len = arr.size
      while(i < len) { arr(i) = (bytes(i) ^ -1).toByte ; i += 1 }
    }
    // If none of the above conditions, we just have the same data as we did before (which was cloned)

    arr
  }

  def mapDouble(f: Double => Double): Array[Byte] =
    map(z => d2i(f(i2d(z))))

  def byteToBinaryString(b: Byte) = {
    val binaryStringBuilder = new StringBuilder()
    for(i <- 0 until 8) {
      binaryStringBuilder.append(if( ((0x80 >>> i) & b) == 0) '0' else '1')
    }
    binaryStringBuilder.toString
  }

  def mapWithIndex(f: (Int, Int) => Int): Array[Byte] = {
    val arr = bytes.clone

    cfor(0)(_ < rows, _ + 1) { row =>
      cfor(0)(_ < cols, _ + 1) { col =>
        val i = row * cols + col
        BitArrayTile.update(arr, index(i), f(i, getInt(i)))
      }
    }

    cfor(0)(_ < arr.size, _ + 1) { i =>
      arr(i) = invertByte(arr(i))
    }

    arr
  }

  def mapDoubleWithIndex(f: (Int, Double) => Double): Array[Byte] = {
    val arr = bytes.clone

    cfor(0)(_ < rows, _ + 1) { row =>
      cfor(0)(_ < cols, _ + 1) { col =>
        val i = row * cols + col
        BitArrayTile.updateDouble(arr, index(i), f(i, getDouble(i)))
      }
    }

    cfor(0)(_ < arr.size, _ + 1) { i =>
      arr(i) = invertByte(arr(i))
    }

    arr
  }
}
