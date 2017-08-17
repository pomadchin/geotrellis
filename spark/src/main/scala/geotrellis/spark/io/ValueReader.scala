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

package geotrellis.spark.io

import geotrellis.spark._
import geotrellis.spark.io.avro._
import geotrellis.spark.io.json._
import spray.json._
import scala.reflect._
import java.net.URI
import java.util.ServiceLoader

/** A key-value reader producer to read a layer one value at a time.
 * This interface abstracts over various construction requirements for
 * constructing a storage back-end specific reader. */
trait ValueReader[ID] {
  val attributeStore: AttributeStore

  /** Produce a key value reader for a specific layer, prefetching layer metadata once at construction time */
  def reader[K: AvroRecordCodec: JsonFormat: ClassTag, V: AvroRecordCodec](layerId: ID): Reader[K, V]
}

object ValueReader {

  def apply(attributeStore: AttributeStore, valueReaderUri: URI): ValueReader[LayerId] = {
    import scala.collection.JavaConversions._
    ServiceLoader.load(classOf[ValueReaderProvider]).iterator()
      .find(_.canProcess(valueReaderUri))
      .getOrElse(throw new RuntimeException(s"Unable to find ValueReaderProvider for $valueReaderUri"))
      .valueReader(valueReaderUri, attributeStore)
  }

  def apply(attributeStoreUri: URI, valueReaderUri: URI): ValueReader[LayerId] =
    apply(AttributeStore(attributeStoreUri), valueReaderUri)

  def apply(uri: URI): ValueReader[LayerId] =
    apply(attributeStoreUri = uri, valueReaderUri = uri)

  def apply(attributeStore: AttributeStore, valueReaderUri: String): ValueReader[LayerId] =
    apply(attributeStore, new URI(valueReaderUri))


  def apply(attributeStoreUri: String, valueReaderUri: String): ValueReader[LayerId] =
    apply(AttributeStore(new URI(attributeStoreUri)), new URI(valueReaderUri))

  def apply(uri: String): ValueReader[LayerId] = {
    val _uri = new URI(uri)
    apply(attributeStoreUri = _uri, valueReaderUri = _uri)
  }
}
