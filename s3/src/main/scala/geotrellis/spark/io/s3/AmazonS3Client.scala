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

import java.io.{BufferedReader, File, InputStreamReader}

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth._
import com.amazonaws.services.s3.{AmazonS3Client => AWSAmazonS3Client}
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.transfer.{TransferManager, TransferManagerBuilder}
import org.apache.commons.io.IOUtils

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.blocking
import scala.concurrent.ExecutionContext.Implicits.global

object AmazonS3Client {
  def apply(s3client: AWSAmazonS3Client): AmazonS3Client =
    new AmazonS3Client(s3client)

  def apply(credentials: AWSCredentials, config: ClientConfiguration): AmazonS3Client =
    apply(new AWSAmazonS3Client(credentials, config))

  def apply(provider: AWSCredentialsProvider, config: ClientConfiguration): AmazonS3Client =
    apply(new AWSAmazonS3Client(provider, config))

  def apply(provider: AWSCredentialsProvider): AmazonS3Client =
    apply(provider, new ClientConfiguration())

}

class AmazonS3Client(s3client: AWSAmazonS3Client) extends S3Client {
  def listObjects(listObjectsRequest: ListObjectsRequest): ObjectListing =
    s3client.listObjects(listObjectsRequest)

  def listKeys(listObjectsRequest: ListObjectsRequest): Seq[String] = {
    var listing: ObjectListing = null
    val result = mutable.ListBuffer[String]()
    do {
      listing = s3client.listObjects(listObjectsRequest)
      // avoid including "directories" in the input split, can cause 403 errors on GET
      result ++= listing.getObjectSummaries.asScala.map(_.getKey).filterNot(_ endsWith "/")
      listObjectsRequest.setMarker(listing.getNextMarker)
    } while (listing.isTruncated)

    result
  }

  def getObject(getObjectRequest: GetObjectRequest): S3Object =
    s3client.getObject(getObjectRequest)

  def putObject(putObjectRequest: PutObjectRequest): PutObjectResult =
    s3client.putObject(putObjectRequest)

  def deleteObject(deleteObjectRequest: DeleteObjectRequest): Unit =
    s3client.deleteObject(deleteObjectRequest)

  def copyObject(copyObjectRequest: CopyObjectRequest): CopyObjectResult =
    s3client.copyObject(copyObjectRequest)

  def listNextBatchOfObjects(listing: ObjectListing): ObjectListing =
    s3client.listNextBatchOfObjects(listing)

  def deleteObjects(deleteObjectsRequest: DeleteObjectsRequest): Unit =
    s3client.deleteObjects(deleteObjectsRequest)

  def readBytes(getObjectRequest: GetObjectRequest): Array[Byte] = {
    val obj = s3client.getObject(getObjectRequest)
    val inStream = obj.getObjectContent
    try {
      IOUtils.toByteArray(inStream)
    } finally {
      inStream.close()
    }
  }

  def timedCreate[T](endMsg: String)(f: => T): T = {
    val s = System.currentTimeMillis
    val result = f
    val e = System.currentTimeMillis
    val t = "%,d".format(e - s)
    println(s"\t$endMsg (in $t ms)")
    result
  }

  //val t = TransferManagerBuilder.defaultTransferManager()

  def splitRange(r: Range, chunks: Int): List[Range] = {
    require(r.step == 1, "Range must have step size equal to 1")
    require(chunks >= 1, "Must ask for at least 1 chunk")

    val dy = r.length
    val dx = chunks

    @tailrec
    def go(y0:Int, y:Int, d:Int, ch:Int, acc: List[Range]):List[Range] = {
      if (ch == 0) acc
      else {
        if (d > 0) go(y0, y-1, d-dx, ch, acc)
        else go(y-1, y, d+dy, ch-1, if (y > y0) acc
        else (y to y0) :: acc)
      }
    }

    go(r.end, r.end, dy - dx, chunks, Nil)
  }

  def splitRangeK[T](r: Range, chunks: Int, func: Range => Future[Array[Byte]]): List[Future[Array[Byte]]] = {
    require(r.step == 1, "Range must have step size equal to 1")
    require(chunks >= 1, "Must ask for at least 1 chunk")

    val dy = r.length
    val dx = chunks

    @tailrec
    def go(y0: Int, y: Int, d: Int, ch: Int, acc: List[Future[Array[Byte]]]): List[Future[Array[Byte]]] = {
      if (ch == 0) acc
      else {
        if (d > 0) go(y0, y-1, d-dx, ch, acc)
        else go(y-1, y, d+dy, ch-1, if (y > y0) acc
        else func(y to y0) :: acc)
      }
    }

    go(r.end, r.end, dy - dx, chunks, Nil)
  }

  def splitRange2(r: (Long, Long), chunks: Int, func: (Long, Long) => Future[Array[Byte]]): List[Future[Array[Byte]]] = {
    require(chunks >= 1, "Must ask for at least 1 chunk")

    val dy = r._2 - r._1
    val dx = chunks

    @tailrec
    def go(y0: Long, y: Long, d: Long, ch: Int, acc: List[Future[Array[Byte]]]): List[Future[Array[Byte]]] = {
      if (ch == 0) acc
      else {
        if (d > 0) go(y0, y-1, d-dx, ch, acc)
        else go(y-1, y, d+dy, ch-1, if (y > y0) acc else func(y, y0) :: acc)
      }
    }

    go(r._2, r._2, dy - dx, chunks, Nil)
  }

  def splitRange3(r: (Long, Long), chunks: Int): List[(Long, Long)] = {
    require(chunks >= 1, "Must ask for at least 1 chunk")

    val dy = r._2 - r._1
    val dx = chunks

    @tailrec
    def go(y0: Long, y: Long, d: Long, ch: Int, acc: List[(Long, Long)]): List[(Long, Long)] = {
      if (ch == 0) acc
      else {
        if (d > 0) go(y0, y-1, d-dx, ch, acc)
        else go(y-1, y, d+dy, ch-1, if (y > y0) acc else (y, y0) :: acc)
      }
    }

    go(r._2, r._2, dy - dx, chunks, Nil)
  }

  def readRange(start: Long, end: Long, getObjectRequest: GetObjectRequest): Array[Byte] = {
    //println(splitRange(start.toInt until end.toInt, math.max(1, (end - start / 66000).toInt)))

    /*splitRange(new Range(start.toInt, end.toInt, 1), math.max(1, (start - end / 66000).toInt)).map {

    }*/

    //println(s"generic start -> end: ${start -> end}")

    //println(s"(end - start / 66000).toInt: ${(end - start / 66000).toInt}")

    //println(s"splitRangeK(${start.toInt} until ${end.toInt}, ${math.max(1, ((end - start) / 66000).toInt)}), { range =>")

    val futures: Seq[Future[Array[Byte]]] =
      splitRange2((start - 1) -> (end - 1), math.max(1, (end - start) / 20000).toInt, { case (s, e) =>
        //val (s, e) = range.start -> range.end
        //println(s"start -> end: ${s -> e}")
        Future {
          blocking {
            val request = getObjectRequest.clone().asInstanceOf[GetObjectRequest]
            request.setRange(s, e)
            //println(s"getObjectRequest.setRange($s, $e): ${e - s}")
            val obj = /*timedCreate("s3client.getObject(getObjectRequest)")(*/ s3client.getObject(request)
            //)
            val stream = /*timedCreate("obj.getObjectContent")(*/ obj.getObjectContent //)
            try {
              //val f = new File("/tmp/binaryspeedtest")
              //timedCreate("tdowanload")(t.download(getObjectRequest, f))
              timedCreate("sun.misc.IOUtils.readFully(stream, -1, true)") {
                sun.misc.IOUtils.readFully(stream, -1, true)
              }
              //IOUtils.toByteArray(stream)
            } finally {
              stream.close()
            }
          }
        }
      })

    Await.result(Future.sequence(futures), Duration.Inf).flatten.toArray

    //val request = getObjectRequest.clone().asInstanceOf[GetObjectRequest]
    /*getObjectRequest.setRange(start, end - 1)
    println(s"getObjectRequest.setRange($start, ${end - 1}): ${end - start}")
    val obj = timedCreate("s3client.getObject(getObjectRequest)")(s3client.getObject(getObjectRequest))
    val stream = timedCreate("obj.getObjectContent")(obj.getObjectContent)

    try {
      //val f = new File("/tmp/binaryspeedtest")
      //timedCreate("tdowanload")(t.download(getObjectRequest, f))
      timedCreate("sun.misc.IOUtils.readFully(stream, -1, true)")(sun.misc.IOUtils.readFully(stream, -1, true))
      //IOUtils.toByteArray(stream)
    } finally {
      stream.close()
    }*/
  }

  def getObjectMetadata(getObjectMetadataRequest: GetObjectMetadataRequest): ObjectMetadata =
    s3client.getObjectMetadata(getObjectMetadataRequest)

  def setRegion(region: com.amazonaws.regions.Region): Unit = {
    s3client.setRegion(region)
  }
}
