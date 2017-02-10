package com.bl.stream

import kafka.message.MessageAndMetadata

/**
  * Created by make on 11/11/16.
  */
trait Handler {

  def handler(messageAndMetadata: MessageAndMetadata[String, String])

}
