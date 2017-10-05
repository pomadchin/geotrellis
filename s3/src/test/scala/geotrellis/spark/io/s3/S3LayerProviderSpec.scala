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

package geotrellis.spark.io.s3

import geotrellis.spark.io._
import geotrellis.spark.testkit.TestEnvironment
import org.scalatest._

class S3LayerProviderSpec extends FunSpec with TestEnvironment {
  val uri = new java.net.URI("s3://fake-bucket/some-prefix")
  it("construct S3AttributeStore from URI"){
    val store = AttributeStore(uri)
    assert(store.isInstanceOf[S3AttributeStore])
  }

  it("construct S3AttributeStore from URI with no prefix"){
    val store = AttributeStore(new java.net.URI("s3://fake-bucket"))
    assert(store.isInstanceOf[S3AttributeStore])
    assert(store.asInstanceOf[S3AttributeStore].prefix != null)
  }


  it("construct S3LayerReader from URI") {
    val reader = LayerReader(uri)
    assert(reader.isInstanceOf[S3LayerReader])
  }

  it("construct S3LayerWriter from URI") {
    val reader = LayerWriter(uri)
    assert(reader.isInstanceOf[S3LayerWriter])
  }

  it("construct S3ValueReader from URI") {
    val reader = ValueReader(uri)
    assert(reader.isInstanceOf[S3ValueReader])
  }
}
