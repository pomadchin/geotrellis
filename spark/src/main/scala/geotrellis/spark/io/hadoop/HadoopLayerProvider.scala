/*
 * Copyright 2017 Azavea
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

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.util.UriUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import java.net.URI

/**
 * Provides [[HadoopAttributeStore]] instance for URI with `hdfs`, `hdfs+file`, `s3n`, `s3a`, `wasb` and `wasbs` schemes.
 * The uri represents Hadoop [[Path]] of catalog root.
 * `wasb` and `wasbs` provide support for the Hadoop Azure connector. Additional
 * configuration is required for this.
 * This Provider intentinally does not handle the `s3` scheme because the Hadoop implemintation is poor.
 * That support is provided by [[S3Attributestore]]
 */
class HadoopLayerProvider extends AttributeStoreProvider
    with LayerReaderProvider with LayerWriterProvider with ValueReaderProvider {
  val schemes: Array[String] = Array("hdfs", "hdfs+file", "s3n", "s3a", "wasb", "wasbs")

  private def trim(uri: URI): URI =
    if (uri.getScheme.startsWith("hdfs+"))
      new URI(uri.toString.stripPrefix("hdfs+"))
    else uri

  def canProcess(uri: URI): Boolean = schemes contains uri.getScheme.toLowerCase

  def attributeStore(uri: URI): AttributeStore = {
    val path = new Path(trim(uri))
    val conf = new Configuration()
    new HadoopAttributeStore(path, conf)
  }

  def layerReader(uri: URI, store: AttributeStore, sc: SparkContext): FilteringLayerReader[LayerId] = {
    // don't need uri because HadoopLayerHeader contains full path of the layer
    new HadoopLayerReader(store)(sc)
  }

  def layerWriter(uri: URI, store: AttributeStore): LayerWriter[LayerId] = {
    val _uri = trim(uri)
    val path = new Path(_uri)
    val params = UriUtils.getParams(_uri)
    val interval = params.getOrElse("interval", "4").toInt
    new HadoopLayerWriter(path, store, interval)
  }

  def valueReader(uri: URI, store: AttributeStore): ValueReader[LayerId] = {
    val _uri = trim(uri)
    val path = new Path(_uri)
    val params = UriUtils.getParams(_uri)
    val conf = new Configuration()
    val maxOpenFiles = params.getOrElse("maxOpenFiles", "16").toInt
    new HadoopValueReader(store, conf, maxOpenFiles)
  }
}
