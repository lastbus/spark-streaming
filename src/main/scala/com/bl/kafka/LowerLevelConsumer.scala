package com.bl.kafka

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions._

/**
  * Created by make on 10/31/16.
  */
object LowerLevelConsumer {

  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "10.201.129.75:9092")
    props.put("group.id", "low-level")
    props.put("enable.auto.commit", "false")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(List("test-00"))
    val minBatchSize = 20
    var count = 0
    while (true) {
      val records = consumer.poll(100)
      for (record <- records) {
        count += 1
        println(
          s"""
             |topic: ${record.topic()}, partition: ${record.partition()}, offset: ${record.offset()}, key: ${record.key()}, value: ${record.value()}


             |
          """.stripMargin)
      }

      if (count > minBatchSize) {

        val offsetCommitCallback = new OffsetCommitCallback {

          override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
            if (exception != null) println("error commit offset: " + exception)
            else {
              println("successfully update kafka consumer offset: ")
              offsets.foreach { case (topicPartition,
              offsetAndMetadata) =>
                println(topicPartition.topic() + " " + topicPartition.partition() + " " + offsetAndMetadata.offset() + " " + offsetAndMetadata

                  .metadata()

                )
              }

            }
          }
        }
        count = 0
        consumer.commitAsync(offsetCommitCallback)

      }
    }
  }

}

