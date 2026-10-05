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

package geotrellis.raster.io.geotiff.compression

import geotrellis.raster.io.geotiff.tags.codes.CompressionType.*

import java.util.zip.{Inflater, Deflater}

/** Compression level: 0 - 9lvl, default is -1, see [[Deflater]] docs for more information */
case class DeflateCompression(level: Int = Deflater.DEFAULT_COMPRESSION) extends Compression {
  def createCompressor(segmentCount: Int): Compressor =
    new Compressor {
      private val segmentSizes = Array.ofDim[Int](segmentCount)
      def compress(segment: Array[Byte], segmentIndex: Int): Array[Byte] = {
        segmentSizes(segmentIndex) = segment.size
        DeflateCompression.deflate(segment, level)
      }

      def createDecompressor(): Decompressor =
        new DeflateDecompressor(segmentSizes)
    }

  def createDecompressor(segmentSizes: Array[Int]): DeflateDecompressor =
    new DeflateDecompressor(segmentSizes)
}

object DeflateCompression extends DeflateCompression(Deflater.DEFAULT_COMPRESSION) {
  /** zlib's compressBound: the max deflated size, so a single pass is enough */
  private def compressBound(length: Int): Int =
    length + (length >> 12) + (length >> 14) + (length >> 25) + 13

  private[compression] def deflate(segment: Array[Byte], level: Int): Array[Byte] = {
    val deflater = new Deflater(level)
    try {
      deflater.setInput(segment, 0, segment.length)
      deflater.finish()
      var result = Array.ofDim[Byte](compressBound(segment.length))
      var length = 0
      // keep deflating until the stream is finished, otherwise incompressible segments get truncated
      while (!deflater.finished()) {
        if (length == result.length) result = java.util.Arrays.copyOf(result, result.length * 2)
        length += deflater.deflate(result, length, result.length - length)
      }
      if (length == result.length) result else java.util.Arrays.copyOf(result, length)
    } finally deflater.end()
  }

  private[compression] def inflate(segment: Array[Byte], size: Int): Array[Byte] = {
    val inflater = new Inflater()
    try {
      inflater.setInput(segment, 0, segment.length)
      val result = Array.ofDim[Byte](size)
      var length = 0
      // a truncated segment stops early and leaves the tail zero filled
      while (length < size && !inflater.finished() && !inflater.needsInput() && !inflater.needsDictionary())
        length += inflater.inflate(result, length, size - length)
      result
    } finally inflater.end()
  }
}

class DeflateDecompressor(segmentSizes: Array[Int]) extends Decompressor {
  def code = ZLibCoded

  def compress(segment: Array[Byte], level: Int = Deflater.DEFAULT_COMPRESSION): Array[Byte] =
    DeflateCompression.deflate(segment, level)

  def decompress(segment: Array[Byte], segmentIndex: Int): Array[Byte] =
    DeflateCompression.inflate(segment, segmentSizes(segmentIndex))
}
