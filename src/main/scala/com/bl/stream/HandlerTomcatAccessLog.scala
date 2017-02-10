package com.bl.stream
import kafka.message.MessageAndMetadata

/**
  * Created by make on 11/11/16.
  */
object HandlerTomcatAccessLog extends Handler {

  override def handler(messageAndMetadata: MessageAndMetadata[String, String]): Unit = {

    val message = messageAndMetadata.message()


  }
}
