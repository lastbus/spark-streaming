package com.bl.stream

import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by make on 11/11/16.
  */
object HBaseTableTomcatAccessLog  extends Serializable {

  val columnFamilyBytes = Bytes.toBytes("info")

  val hostBytes = Bytes.toBytes("host")
  val timeBytes = Bytes.toBytes("time")
  val method = Bytes.toBytes("method")
  val uri = Bytes.toBytes("uri")
  val protocol = Bytes.toBytes("protocol")
  val statusBytes = Bytes.toBytes("status")
  val byte = Bytes.toBytes("byte")
  val timeTaken = Bytes.toBytes("timeTaken")

  val app = Bytes.toBytes("app")
  val interface = Bytes.toBytes("interface")
  val rawData = Bytes.toBytes("rawData")

  val dt = Bytes.toBytes("dt")

  val api = Bytes.toBytes("api")

}
