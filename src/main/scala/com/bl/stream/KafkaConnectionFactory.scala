package com.bl.stream

import java.util.Properties

import org.apache.log4j.Logger
import org.apache.spark.streaming.kafka.KafkaCluster
import scala.collection.JavaConversions._

/**
  * Created by make on 11/1/16.
  */
object KafkaConnectionFactory {
  val logger = Logger.getLogger(this.getClass.getName)

  private var kafkaCluster: KafkaCluster = _


  def getKafkaCluster = {
    if (kafkaCluster == null) {
      val inputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream("kafka.properties")

      if (inputStream == null) {
        logger.error(
          """
            |Cannot found kafka.properties file, please make sure this file is in your classpath.
            |
          """.stripMargin)
        sys.exit(-1)
      }

      val props = new Properties()
      props.load(inputStream)

      logger.info("kafka.properties: ")
      logger.info("-------------------")
      val properties = for ((k, v) <- props if (k != "topic")) yield {
        logger.info(k + ":" + v)
        (k, v)
      }
      kafkaCluster = new KafkaCluster(properties.toMap)
    }
    kafkaCluster
  }

}
