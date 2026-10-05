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

package geotrellis.raster.render.jpg

import geotrellis.raster.*

import java.io.{File, ByteArrayOutputStream}
import javax.imageio.*
import javax.imageio.plugins.jpeg.*
import javax.imageio.stream.*
import java.awt.image.BufferedImage
import java.util.Locale



case class JpgEncoder(settings: Settings = Settings.DEFAULT) {

  def writeParams: ImageWriteParam = {
    val writeParams = new JPEGImageWriteParam(Locale.getDefault())
    writeParams.setCompressionMode(ImageWriteParam.MODE_EXPLICIT)
    writeParams.setCompressionQuality(settings.compressionQuality.toFloat)
    writeParams.setOptimizeHuffmanTables(settings.optimize)
    writeParams
  }

  def writeOutputStream(os: ImageOutputStream, raster: Tile): Unit = {
    val img: BufferedImage = raster.toBufferedImage

    // Write to provided output stream
    val writer: ImageWriter = ImageIO.getImageWritersByFormatName("jpg").next()
    try {
      writer.setOutput(os)
      writer.write(null, new IIOImage(img, null, null), this.writeParams)
    } finally writer.dispose()
  }

  def writeByteArray(raster: Tile): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    // cache in memory: a file cache costs a temp dir per call and the deleteOnExit list never shrinks
    val mcios = new MemoryCacheImageOutputStream(baos)
    try {
      writeOutputStream(mcios, raster)
      mcios.flush()
    } finally mcios.close()
    baos.toByteArray
  }

  def writePath(path: String, raster: Tile): Unit = {
    val fios = new FileImageOutputStream(new File(path))
    try {
      writeOutputStream(fios, raster)
      fios.flush()
    } finally fios.close()
  }
}

