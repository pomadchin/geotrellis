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
import org.apache.accumulo.core.client.security.tokens.PasswordToken

class AccumuloAttributeStoreSpec extends AttributeStoreSpec {
  val accumulo = AccumuloInstance(
    instanceName = "fake",
    zookeeper = "localhost",
    user = "root",
    token = new PasswordToken("")
  )

  lazy val attributeStore = new AccumuloAttributeStore(accumulo.connector, "attributes")
}
