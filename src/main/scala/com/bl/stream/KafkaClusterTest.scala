package com.bl.stream

import kafka.common.TopicAndPartition
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka.KafkaCluster

/**
  * Created by make on 10/31/16.
  */
object KafkaClusterTest {

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.WARN)
    val kafkaProps = Map("bootstrap.servers" -> "10.201.129.75:9092")
    val kafkaCluster = new KafkaCluster(kafkaProps)

    val partitions = kafkaCluster.getPartitions(Set("te00"))

    if (partitions.isLeft) {
      println("""partition is left""")
      throw new Exception("cannot find topic and partitions")
    } else {
      println("""partition is right""")
    }

    val topicAndPartition = partitions.right.get
    val consumerOffset = kafkaCluster.getConsumerOffsets("test", topicAndPartition)
    if (consumerOffset.isLeft) {
      println("consumer is not exists")
      val topicEarliestLeaderOffsets = kafkaCluster.getEarliestLeaderOffsets(topicAndPartition)
      topicEarliestLeaderOffsets.right.get.foreach { case (topicAndPartition, leaderOffset) =>
        println("topic: "  + topicAndPartition.topic + ", partition: " + topicAndPartition.partition + ", leader: " + leaderOffset.host + ":" + leaderOffset.port + ", offset: " + leaderOffset.offset)

      }
      kafkaCluster.setConsumerOffsets("test", topicEarliestLeaderOffsets.right.get.mapValues(s => s.offset))
    }





//    println(kafkaCluster.getPartitionMetadata(Set("test-00")))
//    kafkaCluster.kafkaParams.foreach(println)
//    Console println kafkaCluster.getConsumerOffsets("test", Set(new TopicAndPartition("test-00", 0))).right
//    Console println "find leader " + kafkaCluster.findLeader("test-00", 0)
//    Console println kafkaCluster.getConsumerOffsetMetadata("test", Set(new TopicAndPartition("test-00", 0)))




  }

}
