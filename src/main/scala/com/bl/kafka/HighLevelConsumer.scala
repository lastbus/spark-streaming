package com.bl.kafka

import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConversions._

/**
  * Created by make on 10/31/16.
  */
object HighLevelConsumer {

  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "high-level")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val kafkaConsumer = new KafkaConsumer[String, String](props)
    kafkaConsumer.subscribe(Seq("test"))

    while (true) {
      val records = kafkaConsumer.poll(100)
      for (record <- records) {
        println(
          s"""
            |topic: ${record.topic()}, partition: ${record.partition()}, offset: ${record.offset()}, key: ${record.key()}, value: ${record.value()}
          """.stripMargin)

      }
    }


















  }

}
