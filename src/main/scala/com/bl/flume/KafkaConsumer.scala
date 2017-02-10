package com.bl.flume

import java.util.Properties
import scala.collection.JavaConversions._
import org.apache.kafka.clients.consumer.KafkaConsumer


/**
  * Created by make on 11/11/16.
  */
object KafkaConsumer {

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
    var count = 0
    sys.addShutdownHook(new Thread(new Runnable {
      override def run(): Unit = {
        println("consumer records : " + count)
      }
    }).start())

    while (true) {
      val records = kafkaConsumer.poll(2000)
      count += records.count()
      println("consumer records : " + count)
//      for (record <- records) {
//        println(
//          s"""
//             |topic: ${record.topic()}, partition: ${record.partition()}, offset: ${record.offset()}, key: ${record.key()}, value: ${record.value()}
//          """.stripMargin)
//
//      }
    }




  }

}
