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

package geotrellis.geomesa.geotools

import geotrellis.vector.Geometry
import geotrellis.proj4.{WebMercator, CRS => GCRS}

import com.github.blemale.scaffeine.Scaffeine
import com.vividsolutions.jts.{geom => jts}
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.geomesa.accumulo.index.Constants
import org.opengis.feature.simple.SimpleFeatureType
import com.typesafe.config.ConfigFactory

import scala.reflect._

object GeoMesaSimpleFeatureType {

  val whenField  = GeometryToGeoMesaSimpleFeature.whenField
  val whereField = GeometryToGeoMesaSimpleFeature.whereField

  lazy val featureTypeCache =
    Scaffeine()
      .recordStats()
      .maximumSize(ConfigFactory.load().getInt("geotrellis.geomesa.featureTypeCacheSize"))
      .build[String, SimpleFeatureType]()

  def apply[G <: Geometry: ClassTag](featureName: String, crs: Option[GCRS] = Some(WebMercator), temporal: Boolean = false): SimpleFeatureType = {
    featureTypeCache.get(featureName, { key =>
      val sftb = (new SimpleFeatureTypeBuilder).minOccurs(1).maxOccurs(1).nillable(false)

      sftb.setName(featureName)
      crs.foreach { crs => sftb.setSRS(s"EPSG:${crs.epsgCode.get}") }
      classTag[G].runtimeClass.getName match {
        case "geotrellis.vector.Point" => sftb.add(GeometryToGeoMesaSimpleFeature.whereField, classOf[jts.Point])
        case "geotrellis.vector.Line" => sftb.add(GeometryToGeoMesaSimpleFeature.whereField, classOf[jts.LineString])
        case "geotrellis.vector.Polygon" => sftb.add(GeometryToGeoMesaSimpleFeature.whereField, classOf[jts.Polygon])
        case "geotrellis.vector.MultiPoint" => sftb.add(GeometryToGeoMesaSimpleFeature.whereField, classOf[jts.MultiPoint])
        case "geotrellis.vector.MultiLine" => sftb.add(GeometryToGeoMesaSimpleFeature.whereField, classOf[jts.MultiLineString])
        case "geotrellis.vector.MultiPolygon" => sftb.add(GeometryToGeoMesaSimpleFeature.whereField, classOf[jts.MultiPolygon])
        case g => throw new Exception(s"Unhandled GeoTrellis Geometry $g")
      }
      sftb.setDefaultGeometry(whereField)
      if (temporal) sftb.add(whenField, classOf[java.util.Date])
      val sft = sftb.buildFeatureType
      if (temporal) sft.getUserData.put(Constants.SF_PROPERTY_START_TIME, whenField) // when field is date
      sft
    })
  }
}
