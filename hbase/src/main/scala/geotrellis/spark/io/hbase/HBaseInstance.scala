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

package geotrellis.spark.io.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import java.net.URI

object HBaseInstance {
  def apply(uri: URI): HBaseInstance = {
    import geotrellis.util.UriUtils._

    val zookeeper = uri.getHost
    val port = if (uri.getPort < 0) 2181 else uri.getPort
    val params = getParams(uri)
    HBaseInstance(
      List(zookeeper),
      params.getOrElse("master", ""),
      port.toString)
  }
}

case class HBaseInstance(zookeepers: Seq[String], master: String, clientPort: String = "2181") extends Serializable {
  @transient lazy val conf = {
    val c = HBaseConfiguration.create
    c.set("hbase.zookeeper.quorum", zookeepers.mkString(","))
    c.set("hbase.zookeeper.property.clientPort", clientPort)
    c.set("hbase.master", master)
    c
  }

  def getConnection: Connection = ConnectionFactory.createConnection(conf)
  def getAdmin: Admin = getConnection.getAdmin

  @transient lazy val connection: Connection = getConnection
  @transient lazy val admin: Admin = getAdmin

  /** Without connection close, for a custom connection close */
  def withConnection[T](block: Connection => T): T = block(connection)
  def withAdmin[T](block: Admin => T): T = block(admin)

  /** With connection close */
  def withConnectionDo[T](block: Connection => T): T = {
    val connection = getConnection
    try block(connection) finally connection.close()
  }

  def withTableConnectionDo[T](tableName: TableName)(block: Table => T): T = {
    val connection = getConnection
    val tableConnection = connection.getTable(tableName)
    try block(tableConnection) finally {
      tableConnection.close()
      connection.close()
    }
  }

  def withAdminDo[T](block: Admin => T): T = {
    val connection = getConnection
    val admin = connection.getAdmin
    try block(admin) finally {
      admin.close()
      connection.close()
    }
  }

  def close = {
    admin.close()
    connection.close()
  }
}
