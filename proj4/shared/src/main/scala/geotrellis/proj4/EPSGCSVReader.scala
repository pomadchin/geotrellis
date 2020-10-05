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

package geotrellis.proj4

import com.opencsv.CSVReader

import scala.collection.mutable.ListBuffer

import java.io.InputStreamReader

object CSVFileConstants {

  val CoordRefSysCode        = "COORD_REF_SYS_CODE"
  val CoordRefSysName        = "COORD_REF_SYS_NAME"
  val UOMCode                = "UOM_CODE"
  val CoordOpCode            = "COORD_OP_CODE"
  val CoordOpMethodCode      = "COORD_OP_METHOD_CODE"
  val SourceGeoCRSCode       = "SOURCE_GEOGCRS_CODE"
  val DatumCode              = "DATUM_CODE"
  val PrimeMeridianCode      = "PRIME_MERIDIAN_CODE"
  val FactorBCode            = "FACTOR_B"
  val FactorCCode            = "FACTOR_C"
  val EllipsoidCode          = "ELLIPSOID_CODE"
  val SemiMajorAxisCode      = "SEMI_MAJOR_AXIS"
  val SemiMinorAxisCode      = "SEMI_MINOR_AXIS"
  val InvFlatteningCode      = "INV_FLATTENING"
  val GreenwichLongitudeCode = "GREENWICH_LONGITUDE"

}

object EPSGCSVReader {

  def apply() = new EPSGCSVReader()

}

class EPSGCSVReader {

  import CSVFileConstants._

  def getPrimeMeridianValues(code: Int): Option[Map[String, String]] =
    getValues("prime_meridian.csv", code, PrimeMeridianCode)

  def getEllipsoidValues(code: Int): Option[Map[String, String]] =
    getValues("ellipsoid.csv", code, EllipsoidCode)

  def getDatumValues(code: Int): Option[Map[String, String]] =
    getValues("gdal_datum.csv", code, DatumCode)

  def getUnitOfMeasureValues(code: Int): Option[Map[String, String]] =
    getValues("unit_of_measure.csv", code, UOMCode)

  def getGCSEPSGValues(code: Int): Option[Map[String, String]] = {
    val overrideMap = getValues("gcs.override.csv", code, CoordRefSysCode)
    if (overrideMap != None) overrideMap
    else getValues("gcs.csv", code, CoordRefSysCode)
  }

  def getPCVEPSGValues(code: Int): Option[Map[String, String]] = {
    val overrideMap = getValues("pcs.override.csv", code, CoordRefSysCode)
    if (overrideMap != None) overrideMap
    else getValues("pcs.csv", code, CoordRefSysCode)
  }

  def getProjOpWParmValues(code: Int): Option[Map[String, String]] =
    getValues("projop_wparm.csv", code, CoordOpCode)

  private def getValues(
    fileName: String,
    code: Int,
    codeFlag: String): Option[Map[String, String]] = {
    val resourcePath = s"/proj4/$fileName"
    val stream = getClass.getResourceAsStream(resourcePath)
    if(stream == null) { sys.error(s"Cannot find resource $resourcePath") }
    val reader =
      new CSVReader(new InputStreamReader(stream))
    try {
      val headers = reader.readNext.map(_.toUpperCase)
      val listBuffer = ListBuffer[Map[String, String]]()

      var input: Array[String] = reader.readNext
      var resMap: Option[Map[String, String]] = None
      while (input != null && resMap == None) {
        val map = (headers zip input).toMap
        if (map.get(codeFlag) == Some(code.toString)) resMap = Some(map)
        else input = reader.readNext
      }

      resMap
    } finally {
      reader.close()
    }
  }

}
