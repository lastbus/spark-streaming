package com.bl.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HTable, Table}


/**
  * Created by make on 11/1/16.
  */
object HBaseConnectionFactory extends Serializable {

  // HBase 连接
  private var connection: Connection = _
  // HBase table 连接
  private var table: Table = _

  /**
    * 得到 HBase table 连接
    * @param tableName HBase table 的名称
    * @return HBase table 连接
    */
  def getConnection(tableName: String): Table = {
    if (table == null) {
      val conn = getConnection
      table = conn.getTable(TableName.valueOf(tableName))
      conn.getTable(TableName.valueOf(tableName))
    }
    table
  }

  /**
    * 得到 HBase table 连接
    * @param table HBase table 的名称
    * @return HBase table 连接
    */
  def getConnection2(table: String): Table = {
    val conn = getConnection
    conn.getTable(TableName.valueOf(table))
  }

  /**
    * 得到 HBase table 连接
    * @return HBase 连接
    */
  def getConnection:  Connection = {
    if (connection == null || connection.isClosed) {
      this synchronized  {
        connection = ConnectionFactory.createConnection()
      }
    }
    connection
  }

  /**
    * 关闭 HBase 连接
    */
  sys.addShutdownHook(new Thread {
    override def run(): Unit = {
      println("begin to shutdown hbase connection")
      if (connection != null) {
        connection.close()
      }
      println("finished shutdown hbase connection")
    }
  }.start)

}
