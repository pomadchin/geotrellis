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

package geotrellis.spark.testkit.io

import java.time.ZonedDateTime

import geotrellis.spark._
import geotrellis.spark.io.index._
import jp.ne.opt.chronoscala.Imports._

trait SpaceTimeKeyIndexMethods {
  def keyIndexMethods: Map[String, KeyIndexMethod[SpaceTimeKey]] =
    Map(
      "z order by year" -> ZCurveKeyIndexMethod.byYear,
      "z order by 6 months" -> ZCurveKeyIndexMethod.byMonths(6),
      "hilbert using now" -> HilbertKeyIndexMethod(ZonedDateTime.now - 20.years, ZonedDateTime.now, 4),
      "hilbert resolution" -> HilbertKeyIndexMethod(2)
    )
}
