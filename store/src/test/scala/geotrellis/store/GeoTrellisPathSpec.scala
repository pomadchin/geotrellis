/*
 * Copyright 2019 Azavea
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

package geotrellis.store

import org.scalatest._

class GeoTrellisPathSpec extends FunSpec with Matchers {
  describe("GeoTrellisPathSpec") {
    it("should produce a GeoTrellisPath from a file string") {
      val params = s"?layer=landsat&zoom=0"
      val uriMultiband = s"file:///tmp/catalog$params"

      val path = GeoTrellisPath.parse(uriMultiband)
      path.value shouldBe "file:///tmp/catalog"
      path.layerName shouldBe "landsat"
      path.zoomLevel shouldBe Some(0)
    }

    it("should produce a GeoTrellisPath from an s3 string") {
      val params = s"?layer=landsat&zoom=0"
      val uriMultiband = s"s3://azavea-datahub/catalog${params}"

      val path = GeoTrellisPath.parse(uriMultiband)
      path.value shouldBe "s3://azavea-datahub/catalog"
      path.layerName shouldBe "landsat"
      path.zoomLevel shouldBe Some(0)
    }

    it("should produce a GeoTrellisPath from an s3 string with a host port") {
      val params = s"?layer=landsat&zoom=0"
      val uriMultiband = s"s3://azavea-datahub:666/catalog${params}"

      val path = GeoTrellisPath.parse(uriMultiband)
      path.value shouldBe "s3://azavea-datahub:666/catalog"
      path.layerName shouldBe "landsat"
      path.zoomLevel shouldBe Some(0)
    }

    it("should produce a GeoTrellisPath from an s3 string without a host") {
      val params = s"?layer=landsat&zoom=0"
      val uriMultiband = s"s3://catalog${params}"

      val path = GeoTrellisPath.parse(uriMultiband)
      path.value shouldBe "s3://catalog"
      path.layerName shouldBe "landsat"
      path.zoomLevel shouldBe Some(0)
    }
  }
}
