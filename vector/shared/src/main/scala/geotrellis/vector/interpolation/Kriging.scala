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

package geotrellis.vector.interpolation

import geotrellis.vector._
import org.apache.commons.math3.linear._
import spire.syntax.cfor._
import scala.collection.mutable

trait Kriging extends Function2[Double, Double, (Double, Double)] with Serializable {

  private def distance(p1: Point, p2: Point): Double =
    math.abs(math.sqrt(math.pow(p1.x - p2.x, 2) + math.pow(p1.y - p2.y, 2)))

  protected def varianceMatrixGen(sv: Semivariogram,
                                  points: Array[PointFeature[Double]]): RealMatrix = {
    val n = points.length
    val varianceMatrix: RealMatrix = MatrixUtils.createRealMatrix(n, n)
    cfor(0)(_ < n, _ + 1) { i: Int =>
      varianceMatrix.setEntry(i, i, sv.nugget)
      cfor(i + 1)(_ < n, _ + 1) { j: Int =>
        val dx = points(i).geom.x - points(j).geom.x
        val dy = points(i).geom.y - points(j).geom.y
        val varVal: Double = sv(math.min(math.sqrt(dx * dx + dy * dy), sv.range))
        varianceMatrix.setEntry(i, j, varVal)
        varianceMatrix.setEntry(j, i, varVal)
      }
    }
    varianceMatrix
  }

  /** Returns the indices of points close to the point for prediction within the given bandwidth
    *
    * @note In case the number of points < minPoints; it returns the closest minPoints number of points
    */
  protected def getPointDistancesSorted(points: Array[PointFeature[Double]],
                                        minPoints: Int,
                                        bandwidth: Double,
                                        point: Point): Array[(Int, Double)] = {
    val distances =
      new mutable.PriorityQueue[(Int, Double)]()(Ordering.by(-1 * _._2))

    cfor(0)(_ < points.length, _ + 1) { i =>
      val dVal: Double = distance(points(i), point)
      distances += ((i, dVal))
    }
    val q = distances.dequeueAll
    val result = q.takeWhile(_._2 <= bandwidth).toArray
    if(result.length < minPoints)
      q.take(minPoints).toArray
    else
      result
  }

  /** Returns the absolute values of a given array */
  protected def absArray(arr: Array[Double]): Array[Double] = {
    cfor(0)(_ < arr.length, _ + 1) { i =>
      arr(i) = math.abs(arr(i))
    }
    arr
  }

  private def createPredictor(numberOfPoints: Int): (Double, Double) => (Double, Double) = createPredictorInit(numberOfPoints)
  protected def createPredictorInit(numberOfPoints: Int): (Double, Double) => (Double, Double)

  private lazy val _apply: (Double, Double) => (Double, Double) =
    createPredictor(1)

  def apply(x: Double, y: Double): (Double, Double) =
    _apply(x, y)

  /** Kriging Prediction for an Array of points
   * @param pointMatrix Points to be Kriged
   * @return            Tuples of (krigedValues, krigedVariance) for each of the kriged points
   */
  def predict(pointMatrix: Array[Point]): Array[(Double, Double)] = {
    val krigingPrediction = Array.ofDim[(Double, Double)](pointMatrix.length)
    val predictor = createPredictor(pointMatrix.length)

    cfor(0)(_ < pointMatrix.length, _ + 1) { i: Int =>
      val pointPredict: Point = pointMatrix(i)
      krigingPrediction(i) = predictor(pointPredict.x, pointPredict.y)
    }

    krigingPrediction
  }
}
