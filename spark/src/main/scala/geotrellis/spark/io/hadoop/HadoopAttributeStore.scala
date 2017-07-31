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

package geotrellis.spark.io.hadoop

import geotrellis.spark._
import geotrellis.spark.io._

import spray.json._
import DefaultJsonProtocol._
import org.apache.hadoop.fs.Path
import org.apache.spark._
import org.apache.hadoop.conf.Configuration

import java.io.PrintWriter
import scala.util.matching.Regex

class HadoopAttributeStore(val rootPath: Path, val hadoopConfiguration: Configuration) extends BlobLayerAttributeStore {
  import HadoopAttributeStore._

  val (fs, attributePath) = {
    val ap = new Path(rootPath, "_attributes")
    val fs = ap.getFileSystem(hadoopConfiguration)

    // Create directory if it doesn't exist
    if(!fs.exists(ap)) fs.mkdirs(ap)

    // Get the absolute path to attributes
    (fs, fs.getFileStatus(ap).getPath)
  }

  def attributePath(layerId: LayerId, attributeName: String): Path = {
    val fname = s"${layerId.name}${SEP}${layerId.zoom}${SEP}${attributeName}.json"
    new Path(attributePath, fname)
  }

  private def delete(layerId: LayerId, path: Path): Unit = {
    HdfsUtils
      .listFiles(new Path(attributePath, path), hadoopConfiguration)
      .foreach(fs.delete(_, false))
    clearCache(layerId)
  }

  def attributeWildcard(attributeName: String): Path =
    new Path(s"*${SEP}${attributeName}.json")

  def layerWildcard(layerId: LayerId): Path =
    new Path(s"${layerId.name}${SEP}${layerId.name}${SEP}*.json")

  private def readFile[T: JsonFormat](path: Path): Option[(LayerId, T)] = {
    HdfsUtils
      .getLineScanner(path, hadoopConfiguration)
      .map{ in =>
        val txt =
          try {
            in.mkString
          }
          finally {
            in.close()
          }
        txt.parseJson.convertTo[(LayerId, T)]
      }
  }

  def read[T: JsonFormat](layerId: LayerId, attributeName: String): T =
    readFile[T](attributePath(layerId, attributeName)) match {
      case Some((id, value)) => value
      case None => throw new AttributeNotFoundError(attributeName, layerId)
    }

  def readAll[T: JsonFormat](attributeName: String): Map[LayerId,T] = {
    HdfsUtils
      .listFiles(attributeWildcard(attributeName), hadoopConfiguration)
      .map{ path: Path =>
        readFile[T](path) match {
          case Some(tup) => tup
          case None => throw new LayerIOError(s"Unable to list $attributeName attributes from $path")
        }
      }
      .toMap
  }

  def write[T: JsonFormat](layerId: LayerId, attributeName: String, value: T): Unit = {
    val path = attributePath(layerId, attributeName)

    if(fs.exists(path)) {
      fs.delete(path, false)
    }

    val fdos = fs.create(path)
    val out = new PrintWriter(fdos)
    try {
      val s = (layerId, value).toJson.toString()
      out.println(s)
    } finally {
      out.close()
      fdos.close()
    }
  }

  def layerExists(layerId: LayerId): Boolean = {
    // Use a relative path, since
    // the listFiles could return a different host name.
    val metadataRelativePath =
      attributePath(layerId, AttributeStore.Fields.metadata).toUri.getPath
    HdfsUtils
      .listFiles(new Path(attributePath, s"*.json"), hadoopConfiguration)
      .exists { _.toUri.getPath ==  metadataRelativePath }
  }

  def delete(layerId: LayerId): Unit = {
    delete(layerId, new Path(s"${layerId.name}${SEP}${layerId.zoom}${SEP}*.json"))
    clearCache(layerId)
  }

  def delete(layerId: LayerId, attributeName: String): Unit = {
    delete(layerId, new Path(s"${layerId.name}${SEP}${layerId.zoom}${SEP}${attributeName}.json"))
    clearCache(layerId, attributeName)
  }

  def layerIds: Seq[LayerId] =
    HdfsUtils
      .listFiles(new Path(attributePath, s"*.json"), hadoopConfiguration)
      .map { path: Path =>
        val List(name, zoomStr) = path.getName.split(SEP).take(2).toList
        LayerId(name, zoomStr.toInt)
      }
      .distinct

  def availableAttributes(layerId: LayerId): Seq[String] = {
    HdfsUtils
      .listFiles(layerWildcard(layerId), hadoopConfiguration)
      .map { path: Path =>
        val attributeRx(name, zoom, attribute) = path.getName
        attribute
      }
      .toVector
  }
}

object HadoopAttributeStore {
  final val SEP = "___"

  val attributeRx = {
    val slug = "[a-zA-Z0-9-]+"
    new Regex(s"""($slug)$SEP($slug)${SEP}($slug).json""", "layer", "zoom", "attribute")
  }

  def apply(rootPath: Path, config: Configuration): HadoopAttributeStore =
    new HadoopAttributeStore(rootPath, config)

  def apply(rootPath: Path)(implicit sc: SparkContext): HadoopAttributeStore =
    apply(rootPath, sc.hadoopConfiguration)
}
