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

package geotrellis.spark.io.accumulo

import geotrellis.spark.io._
import geotrellis.spark.testkit.TestEnvironment
import org.scalatest._

class AccumuloLayerProviderSpec extends FunSpec with TestEnvironment {
  val uri = new java.net.URI("accumulo://root:@localhost/fake?attributes=attributes&layers=tiles")
  it("construct AccumuloAttributeStore from URI"){
    val store = AttributeStore(uri)
    assert(store.isInstanceOf[AccumuloAttributeStore])
  }

  it("construct AccumuloLayerReader from URI") {
    val reader = LayerReader(uri)
    assert(reader.isInstanceOf[AccumuloLayerReader])
  }

  it("construct AccumuloLayerWriter from URI") {
    val reader = LayerWriter(uri)
    assert(reader.isInstanceOf[AccumuloLayerWriter])
  }

  it("construct AccumuloValueReader from URI") {
    val reader = ValueReader(uri)
    assert(reader.isInstanceOf[AccumuloValueReader])
  }

}
