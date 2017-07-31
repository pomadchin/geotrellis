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

package geotrellis.spark.io.hadoop

import geotrellis.raster.io.geotiff.reader._
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, SinglebandGeoTiff}
import geotrellis.vector.Extent

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext

object HadoopGeoTiffReader {
  def readSingleband(path: Path)(implicit sc: SparkContext): SinglebandGeoTiff = readSingleband(path, decompress = true, streaming = false, None, sc.hadoopConfiguration)
  def readSingleband(path: Path, decompress: Boolean, streaming: Boolean, extent: Option[Extent], conf: Configuration): SinglebandGeoTiff =
    HdfsUtils.read(path, conf) { is =>
      val geoTiff = GeoTiffReader.readSingleband(IOUtils.toByteArray(is), decompress, streaming)
      extent match {
        case Some(e) => geoTiff.crop(e)
        case _ => geoTiff
      }
    }

  def readMultiband(path: Path)(implicit sc: SparkContext): MultibandGeoTiff = readMultiband(path, decompress = true, streaming = false, None, sc.hadoopConfiguration)
  def readMultiband(path: Path, decompress: Boolean, streaming: Boolean, extent: Option[Extent], conf: Configuration): MultibandGeoTiff =
    HdfsUtils.read(path, conf) { is =>
      val geoTiff = GeoTiffReader.readMultiband(IOUtils.toByteArray(is), decompress, streaming)
      extent match {
        case Some(e) => geoTiff.crop(e)
        case _ => geoTiff
      }
    }
}
