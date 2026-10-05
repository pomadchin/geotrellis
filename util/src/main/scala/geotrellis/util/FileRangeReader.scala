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

package geotrellis.util

import java.io.*
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption

/**
 * This class extends [[RangeReader]] by reading chunks from a given local path. This
 * allows for reading in of files larger than 4gb into GeoTrellis.
 *
 * @param file: A local File to read bytes from.
 * @return A new instance of FileRangeReader
 */
class FileRangeReader(val file: File) extends RangeReader {
  val totalLength: Long = file.length

  def readClippedRange(start: Long, length: Int): Array[Byte] = {
    // read into the heap directly: a mapped buffer would only be unmapped once GC collects it
    val channel = FileChannel.open(file.toPath, StandardOpenOption.READ)
    try {
      val data = Array.ofDim[Byte](length)
      Filesystem.readFully(channel, ByteBuffer.wrap(data), start)
      data
    } finally channel.close()
  }
}

/** The companion object of [[FileRangeReader]] */
object FileRangeReader {

  /**
   * Returns a new instance of FileRangeReader.
   *
   * @param path: A String that is the path to the local file.
   * @return A new instance of FileRangeReader
   */
  def apply(path: String): FileRangeReader =
    new FileRangeReader(new File(path))

  /**
    * Returns a new instance of FileRangeReader.
    *
    * @param file: A local File to read bytes from.
    * @return A new instance of FileRangeReader
    */
  def apply(file: File): FileRangeReader =
    new FileRangeReader(file)
}
