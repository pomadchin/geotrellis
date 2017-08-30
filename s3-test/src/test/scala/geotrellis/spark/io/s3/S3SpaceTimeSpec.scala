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

package geotrellis.spark.io.s3

import geotrellis.raster.Tile
import geotrellis.spark.io._
import geotrellis.spark.io.s3.testkit._
import geotrellis.spark.io.index._
import geotrellis.spark._
import geotrellis.spark.testkit.io._
import geotrellis.spark.testkit.testfiles.TestFiles
import geotrellis.spark.testkit.TestEnvironment

import org.scalatest._

class S3SpaceTimeSpec
  extends PersistenceSpec[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]]
    with SpaceTimeKeyIndexMethods
    with TestEnvironment
    with TestFiles
    with CoordinateSpaceTimeSpec
    with LayerUpdateSpaceTimeTileSpec
    with BeforeAndAfterAll {

  registerAfterAll { () =>
    MockS3Client.reset()
  }

  lazy val bucket = "mock-bucket"
  lazy val prefix = "catalog"

  lazy val attributeStore = new S3AttributeStore(bucket, prefix) {
    override val s3Client = new MockS3Client
  }

  lazy val rddReader =
    new S3RDDReader {
      def getS3Client = () => new MockS3Client()
    }

  lazy val rddWriter =
    new S3RDDWriter {
    def getS3Client = () => new MockS3Client
  }

  lazy val reader = new MockS3LayerReader(attributeStore)
  lazy val creader = new MockS3CollectionLayerReader(attributeStore)
  lazy val writer = new MockS3LayerWriter(attributeStore, bucket, prefix)
  lazy val updater = new S3LayerUpdater(attributeStore, reader) { override def rddWriter = S3SpaceTimeSpec.this.rddWriter }
  lazy val deleter = new S3LayerDeleter(attributeStore) { override val getS3Client = () => new MockS3Client }
  lazy val copier = new S3LayerCopier(attributeStore, bucket, prefix) { override val getS3Client = () => new MockS3Client }
  lazy val reindexer = GenericLayerReindexer[S3LayerHeader](attributeStore, reader, writer, deleter, copier)
  lazy val mover = GenericLayerMover(copier, deleter)
  lazy val tiles = new S3ValueReader(attributeStore) {
    override val s3Client = new MockS3Client
  }
  lazy val sample =  CoordinateSpaceTime
}
