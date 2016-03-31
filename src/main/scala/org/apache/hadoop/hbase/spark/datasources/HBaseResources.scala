/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.spark.datasources

import java.util

import org.apache.hadoop.hbase.{ServerName, HRegionInfo, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.spark.HBaseRelation

import scala.language.implicitConversions

// Resource and ReferencedResources are defined for extensibility,
// e.g., consolidate scan and bulkGet in the future work.

// User has to invoke release explicitly to release the resource,
// and potentially parent resources
trait Resource {
  def release(): Unit
}

case class ScanResource(tbr: TableResource, rs: ResultScanner) extends Resource {
  def release() {
    rs.close()
    tbr.release()
  }
}

case class GetResource(tbr: TableResource, rs: Array[Result]) extends Resource {
  def release() {
    tbr.release()
  }
}

trait ReferencedResource {
  var count: Int = 0
  def init(): Unit
  def destroy(): Unit
  def acquire() = synchronized {
    try {
      count += 1
      if (count == 1) {
        init()
      }
    } catch {
      case e: Throwable =>
        release()
        throw e
    }
  }

  def release() = synchronized {
    count -= 1
    if (count == 0) {
      destroy()
    }
  }

  def releaseOnException[T](func: => T): T = {
    acquire()
    val ret = {
      try {
        func
      } catch {
        case e: Throwable =>
          release()
          throw e
      }
    }
    ret
  }
}

case class TableResource(relation: HBaseRelation) extends ReferencedResource {
  var connection: HConnection = _
  var table: HTable = _
  init()

  override def init(): Unit = {
    val admin = new HBaseAdmin(relation.hbaseConf)
    connection = admin.getConnection;
    //connection = ConnectionFactory.createConnection(relation.hbaseConf)
    //table = connection.getTable(TableName.valueOf(relation.tableName))
    table = new HTable(relation.hbaseConf, relation.tableName)
  }

  override def destroy(): Unit = {
    if (table != null) {
      table.close()
      table = null
    }
    if (connection != null) {
      connection.close()
      connection = null
    }
  }

  def getScanner(scan: Scan): ScanResource = releaseOnException {
    ScanResource(this, table.getScanner(scan))
  }

  def get(list: java.util.List[org.apache.hadoop.hbase.client.Get]) = releaseOnException {
    GetResource(this, table.get(list))
  }
}

case class RegionResource(relation: HBaseRelation) extends ReferencedResource {
  var connection: HConnection = _
  //var rl: RegionLocator = _
  var r1 : util.NavigableMap[HRegionInfo, ServerName] = _

  init()
  println("------getting region resource-----------")
  println(connection)
  println(r1.size())
  println("------getting region resource-----------")
  
  val regions = releaseOnException {

    val infos = r1.keySet().toArray
    val starts = infos.map(_.asInstanceOf[HRegionInfo].getStartKey)
    val ends = infos.map(_.asInstanceOf[HRegionInfo].getEndKey)
    //val hostNameArray = r1.values().toArray()

    starts.zip(ends)
      .zipWithIndex
      .map(x =>
        HBaseRegion(x._2,
          Some(x._1._1),
          Some(x._1._2),
          Some(r1.get(infos(x._2)).getHostname)
        ))

    /*val keys = rl.getStartEndKeys
    keys.getFirst.zip(keys.getSecond)
      .zipWithIndex
      .map(x =>
      HBaseRegion(x._2,
        Some(x._1._1),
        Some(x._1._2),
        Some(rl.getRegionLocation(x._1._1).getHostname)))*/
  }

  override def init(): Unit = {
    connection = new HBaseAdmin(relation.hbaseConf).getConnection
    //connection = ConnectionFactory.createConnection(relation.hbaseConf)
    val table = new HTable(relation.hbaseConf, TableName.valueOf(relation.tableName))
    r1 = table.getRegionLocations;
    //val location = connection.getRegionLocation(TableName.valueOf(relation.tableName), true)

    //rl = connection.getRegionLocator(TableName.valueOf(relation.tableName))
  }

  override def destroy(): Unit = {
    /*if (rl != null) {
      rl.close()
      rl = null
    }*/
    if (connection != null) {
      connection.close()
      connection = null
    }
  }
}

object HBaseResources{
  implicit def ScanResToScan(sr: ScanResource): ResultScanner = {
    sr.rs
  }

  implicit def GetResToResult(gr: GetResource): Array[Result] = {
    gr.rs
  }

  implicit def TableResToTable(tr: TableResource): HTable = {
    tr.table
  }

  implicit def RegionResToRegions(rr: RegionResource): Seq[HBaseRegion] = {
    rr.regions
  }
}
