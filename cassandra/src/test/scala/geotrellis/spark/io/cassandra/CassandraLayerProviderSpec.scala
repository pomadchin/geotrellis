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

package geotrellis.spark.io.cassandra

import geotrellis.spark.io._
import geotrellis.spark.CassandraTestEnvironment
import geotrellis.spark.testkit.TestEnvironment
import org.scalatest._

class CassandraLayerProviderSpec extends FunSpec with CassandraTestEnvironment {
  val uri = new java.net.URI("cassandra://127.0.0.1/geotrellis?attributes=attributes&layers=tiles")
  it("construct CassandraAttributeStore from URI"){
    val store = AttributeStore(uri)
    assert(store.isInstanceOf[CassandraAttributeStore])
  }

  it("construct CassandraLayerReader from URI") {
    val reader = LayerReader(uri)
    assert(reader.isInstanceOf[CassandraLayerReader])
  }

  it("construct CassandraLayerWriter from URI") {
    val reader = LayerWriter(uri)
    assert(reader.isInstanceOf[CassandraLayerWriter])
  }

  it("construct CassandraValueReader from URI") {
    val reader = ValueReader(uri)
    assert(reader.isInstanceOf[CassandraValueReader])
  }

}
