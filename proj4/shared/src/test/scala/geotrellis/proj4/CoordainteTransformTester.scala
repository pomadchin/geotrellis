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

import org.locationtech.proj4j._

class CoordinateTransformTester(val verbose: Boolean = true) {
  val ctFactory = new CoordinateTransformFactory()
  val crsFactory = new CRSFactory()

  val WGS84_PARAM = "+title=long/lat:WGS84 +proj=longlat +datum=WGS84 +units=degrees"
  val WGS84 = crsFactory.createFromParameters("WGS84", WGS84_PARAM)

  private val p = new ProjCoordinate()
  private val p2 = new ProjCoordinate()

  def checkTransformFromWGS84(name: String, lon: Double, lat: Double, x: Double, y: Double): Boolean =
    checkTransformFromWGS84(name, lon, lat, x, y, 0.0001)

  def checkTransformFromWGS84(name: String, lon: Double, lat: Double, x: Double, y: Double, tolerance: Double): Boolean =
    checkTransform(WGS84, lon, lat, createCRS(name), x, y, tolerance)

  def checkTransformToWGS84(name: String, x: Double, y: Double, lon: Double, lat: Double, tolerance: Double): Boolean =
    checkTransform(createCRS(name), x, y, WGS84, lon, lat, tolerance)

  def checkTransformFromGeo(name: String, lon: Double, lat: Double, x: Double, y: Double, tolerance: Double): Boolean = {
    val crs = createCRS(name)
    val geoCRS = crs.createGeographic()
    checkTransform(geoCRS, lon, lat, crs, x, y, tolerance)
  }

  def checkTransformToGeo(name: String, x: Double, y: Double, lon: Double, lat: Double, tolerance: Double): Boolean = {
    val  crs = createCRS(name)
    val geoCRS = crs.createGeographic()
    checkTransform(crs, x, y, geoCRS, lon, lat, tolerance)
  }

  def createCRS(crsSpec: String): CoordinateReferenceSystem =
    // test if name is a PROJ4 spec
    if (crsSpec.indexOf("+") >= 0 || crsSpec.indexOf("=") >= 0)
      crsFactory.createFromParameters("Anon", crsSpec)
    else
      crsFactory.createFromName(crsSpec)

  def crsDisplay(crs: CoordinateReferenceSystem): String =
    crs.getName() + "(" + crs.getProjection()+"/" + crs.getDatum().getCode() + ")"

  def checkTransform(
    srcCRS: String, x1: Double, y1: Double,
    tgtCRS: String, x2: Double, y2: Double, tolerance: Double
  ): Boolean = {
    return checkTransform(
      createCRS(srcCRS), x1, y1,
      createCRS(tgtCRS), x2, y2, tolerance)
  }

  def checkTransform(
    srcCRS: String, p1: ProjCoordinate,
    tgtCRS: String, p2: ProjCoordinate, tolerance: Double
  ): Boolean =
    checkTransform(
      createCRS(srcCRS), p1,
      createCRS(tgtCRS), p2, tolerance)

  def checkTransform(
    srcCRS: CoordinateReferenceSystem, x1: Double, y1: Double,
    tgtCRS: CoordinateReferenceSystem, x2: Double, y2: Double,
    tolerance: Double
  ): Boolean =
    checkTransform(
      srcCRS, new ProjCoordinate(x1, y1),
      tgtCRS, new ProjCoordinate(x2, y2),
      tolerance)

  def checkTransform(
    srcCRS: CoordinateReferenceSystem, p: ProjCoordinate,
    tgtCRS: CoordinateReferenceSystem, p2: ProjCoordinate, tolerance: Double
  ): Boolean = {
    val trans = ctFactory.createTransform(srcCRS, tgtCRS)
    val pout = new ProjCoordinate()
    trans.transform(p, pout)

    val dx = math.abs(pout.x - p2.x)
    val dy = math.abs(pout.y - p2.y)
    val delta = math.max(dx, dy)

    if (verbose) {
      System.out.println(crsDisplay(srcCRS) + " => " + crsDisplay(tgtCRS) )
      System.out.println(
      	p.toShortString()
          + " -> "
          + pout.toShortString()
          + " (expected: " + p2.toShortString()
          + " tol: " + tolerance + " diff: " + delta
          + " )"
      )
    }

    val isInTol = delta <= tolerance

    if (verbose && ! isInTol) {
      System.out.println("FAIL")
      System.out.println("Src CRS: " + srcCRS.getParameterString())
      System.out.println("Tgt CRS: " + tgtCRS.getParameterString())
    }

    if (verbose) {
      System.out.println()
    }

    isInTol
  }

  /**
    * Checks forward and inverse transformations between
    * two coordinate systems for a given pair of points.
    *
    * @param cs1
    * @param x1
    * @param y1
    * @param cs2
    * @param x2
    * @param y2
    * @param tolerance
    * @param checkInverse
    * @return
    */
  def checkTransform(
    cs1: String, x1: Double, y1: Double,
    cs2: String, x2: Double, y2: Double,
    tolerance: Double, inverseTolerance: Double,
    checkInverse: Boolean): Boolean = {
    val isOkForward = checkTransform(cs1, x1, y1, cs2, x2, y2, tolerance)
    val isOkInverse =
      if (checkInverse)
        checkTransform(cs2, x2, y2, cs1, x1, y1, inverseTolerance)
      else true

    isOkForward && isOkInverse
  }
}
