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

package geotrellis.spark.io.hbase

import geotrellis.spark.LayerId
import geotrellis.spark.io._
import geotrellis.spark.io.avro.codecs.KeyValueRecordCodec
import geotrellis.spark.io.avro.{AvroEncoder, AvroRecordCodec}

import org.apache.hadoop.hbase.client.Get
import spray.json._

import scala.reflect.ClassTag

class HBaseValueReader(
  instance: HBaseInstance,
  val attributeStore: AttributeStore
) extends ValueReader[LayerId] {

  def reader[K: AvroRecordCodec: JsonFormat: ClassTag, V: AvroRecordCodec](layerId: LayerId): Reader[K, V] = new Reader[K, V] {
    val header = attributeStore.readHeader[HBaseLayerHeader](layerId)
    val keyIndex = attributeStore.readKeyIndex[K](layerId)
    val writerSchema = attributeStore.readSchema(layerId)
    val codec = KeyValueRecordCodec[K, V]

    def read(key: K): V = instance.withTableConnectionDo(header.tileTable) { table =>
      val get = new Get(HBaseKeyEncoder.encode(layerId, keyIndex.toIndex(key)))
      get.addFamily(HBaseRDDWriter.tilesCF)
      val row = table.get(get)
      val tiles: Vector[(K, V)] =
        AvroEncoder
          .fromBinary(writerSchema, row.getValue(HBaseRDDWriter.tilesCF, ""))(codec)
          .filter(pair => pair._1 == key)

      if (tiles.isEmpty) {
        throw new ValueNotFoundError(key, layerId)
      } else if (tiles.size > 1) {
        throw new LayerIOError(s"Multiple values (${tiles.size}) found for $key for layer $layerId")
      } else {
        tiles.head._2
      }
    }
  }
}

object HBaseValueReader {
  def apply[K: AvroRecordCodec: JsonFormat: ClassTag, V: AvroRecordCodec](
    instance: HBaseInstance,
    attributeStore: AttributeStore,
    layerId: LayerId
  ): Reader[K, V] =
    new HBaseValueReader(instance, attributeStore).reader[K, V](layerId)

  def apply(instance: HBaseInstance): HBaseValueReader =
    new HBaseValueReader(
      instance = instance,
      attributeStore = HBaseAttributeStore(instance))

  def apply(attributeStore: HBaseAttributeStore): HBaseValueReader =
    new HBaseValueReader(
      instance = attributeStore.instance,
      attributeStore = attributeStore)
}
