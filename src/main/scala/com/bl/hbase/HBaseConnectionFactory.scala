package com.bl.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HTable, Table}


/**
  * Created by make on 11/1/16.
  */
object HBaseConnectionFactory extends Serializable {

  private var connection: Connection = _
  private var table: Table = _

  def getConnection(tableName: String): Table = {
    if (table == null) {
      val conn = getConnection
      table = conn.getTable(TableName.valueOf(tableName))
      conn.getTable(TableName.valueOf(tableName))
    }
    table
  }

  def getConnection2(table: String): Table = {
    val conn = getConnection
    conn.getTable(TableName.valueOf(table))
  }

  def getConnection:  Connection = {
    if (connection == null || connection.isClosed) {
      this synchronized  {
        connection = ConnectionFactory.createConnection()
      }
    }
    connection
  }

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
