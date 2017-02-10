package com.bl.member

import org.apache.hadoop.hbase.client.Put

/**
  * Created by make on 1/18/17.
  */
trait JSONParser {

  def parse(json: String): Put

}
