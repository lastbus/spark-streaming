package com.bl.util

import com.bl.stream.HBaseTableTomcatAccessLog
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.LogManager

/**
  * Created by make on 11/14/16.
  */
object URLMatcher {

  val logger = LogManager.getLogger(this.getClass)

  val common = "/recommend/"
  val recRegex = (common + "rec\\?*(.*)").r
  val deprecatedRegex = (common + "([^(rec)].+)").r
  val keyMapping = FileUtil.getPropertiesMap("urlKeyMapping.properties").map { case (key, value) => (key, Bytes.toBytes(value))}


  def parse(url: String, put: Put): Unit = {

    url match {

      case deprecatedRegex(u) =>
        val array = u.split("\\?")
        put.addColumn(HBaseTableTomcatAccessLog.columnFamilyBytes, HBaseTableTomcatAccessLog.api, Bytes.toBytes(array(0)))
        if (array.length == 2) {
          val kvs = array(1).split("&")
          kvs.foreach { kv =>
            val t = kv.split("=")
            if (t.length == 2) {
              put.addColumn(HBaseTableTomcatAccessLog.columnFamilyBytes, keyMapping.getOrElse(t(0), Bytes.toBytes(t(0))), Bytes.toBytes(t(1)))
            }
          }
        }
      case recRegex(pars) =>
        if (!pars.isEmpty) {
          val kvs = pars.split("&")
          kvs.foreach { kv =>
            val t = kv.split("=")
            if (t.length == 2) {
              put.addColumn(HBaseTableTomcatAccessLog.columnFamilyBytes, keyMapping.getOrElse(t(0), Bytes.toBytes(t(0))), Bytes.toBytes(t(1)))
            }
          }
        } else {
          put.addColumn(HBaseTableTomcatAccessLog.columnFamilyBytes, HBaseTableTomcatAccessLog.api, Bytes.toBytes("not-known"))
        }
      case e =>
        logger.info(e)
    }

  }


}
