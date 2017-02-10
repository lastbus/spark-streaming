package com.bl.member

import com.bl.hbase.HBaseConnectionFactory
import com.bl.util.FileUtil
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.json.JSONObject

import scala.collection.JavaConversions._


/**
  * Created by make on 1/18/17.
  */
object MemberStreaming {

  val logger = Logger.getLogger(this.getClass.getName)

  // member data's kafka topic
  val topic = "memberTopic"

  def main(args: Array[String]): Unit = {

    var seconds = 10
    for (i <- 0 until args.length if i % 2 == 0) {
      args(i) match {
        case "--interval" => seconds = args(i + 1).toInt
        case e =>
      }
    }

    val propertiesMap = FileUtil.getPropertiesMap("memberStreaming.properties")
    val groupId = propertiesMap("group.id")

    val kafkaCluster = new KafkaCluster(propertiesMap)


    val partitionsE = kafkaCluster.getPartitions(Set(topic))
    if (partitionsE.isLeft) {
      throw new SparkException(s"get kafka topic ${topic} partitions and metadata failed: ${partitionsE.left.get} ")
    }
    val partitions = partitionsE.right.get
    val consumerOffsetsE = kafkaCluster.getConsumerOffsets(groupId, partitions)

    if (consumerOffsetsE.isLeft) {
      // first consume
      val reset = propertiesMap.get("auto.offset.reset").map(_.toLowerCase)
      val leaderOffsets: Map[TopicAndPartition, LeaderOffset] =
        if (reset == Some("smallest")) {
          val leaderOffsetsE = kafkaCluster.getEarliestLeaderOffsets(partitions)
          if (leaderOffsetsE.isLeft)
            throw new SparkException(s"get earliest leader offsets failed: ${leaderOffsetsE.left.get}")
          leaderOffsetsE.right.get
        } else {
          val leaderOffsetsE = kafkaCluster.getLatestLeaderOffsets(partitions)
          if (leaderOffsetsE.isLeft)
            throw new SparkException(s"get latest leader offsets failed: ${leaderOffsetsE.left.get}")
          leaderOffsetsE.right.get
        }

      val offsets = leaderOffsets.map {
        case (tp, offset) => (tp, offset.offset)
      }

      kafkaCluster.setConsumerOffsets(groupId, offsets)


    } else {

      val earliestLeaderOffsetsE = kafkaCluster.getEarliestLeaderOffsets(partitions)
      if (earliestLeaderOffsetsE.isLeft) {
        throw new SparkException(s"get earliest leader offsets failed: ${earliestLeaderOffsetsE.left.get}")
      }
      val earliestLeaderOffsets = earliestLeaderOffsetsE.right.get
      val consumerOffsets = consumerOffsetsE.right.get
      // 可能只是存在部分分区 consumerOffsets 过时，所以只更新过时分区的 consumerOffsets 为 earliestLeaderOffsets
      var offsets: Map[TopicAndPartition, Long] = Map()
      consumerOffsets.foreach({ case(tp, n) =>
        val earliestLeaderOffset = earliestLeaderOffsets(tp).offset
        if (n < earliestLeaderOffset) {
          println("consumer group:" + groupId + ",topic:" + tp.topic + ",partition:" + tp.partition +
            " offsets已经过时，更新为" + earliestLeaderOffset)
          offsets += (tp -> earliestLeaderOffset)
        }
      })
      if (!offsets.isEmpty) {
        kafkaCluster.setConsumerOffsets(groupId, offsets)
      }
    }

    val topicAndPartitionSet = kafkaCluster.getPartitions(Set(topic)).right.get
    val topicAndPartitionMap = kafkaCluster.getConsumerOffsets(groupId, topicAndPartitionSet).right.get

    val sparkConf = new SparkConf()
    //sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "1000") this can be set in command line
    val ssc = new StreamingContext(sparkConf, Seconds(seconds))

    val message =  KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, Int, Long, String, String)](
      ssc, propertiesMap, topicAndPartitionMap, (mm: MessageAndMetadata[String, String]) => (mm.topic, mm.partition, mm.offset, mm.key(), mm.message()))

    var offsetRanges = Array[OffsetRange]()
    MemberTopicParser.init()

    message.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.foreachRDD { rdd =>

      rdd.foreachPartition { partitions =>
        val hTable = HBaseConnectionFactory.getConnection("member_register")

        val puts =
        partitions.map { case (topic0, partition, offset, k, v) =>
          logger.debug(s"topic:$topic0, partition: $partition, offset: $offset, key: $k, value: $v")
          val put =
          try {
            topic0 match {
              case "memberTopic" => MemberTopicParser.parse(v)
              case e =>
                logger.error(e)
                null
            }

          } catch {
            case e: Exception =>
              logger.error(e)
              null
          }
            put
        }.toList

        hTable.put(puts)

      }

      // update topic offset
      offsetRanges.foreach { offsetRanges =>
        logger.warn(
          s"""
             |topic: ${offsetRanges.topic}, partition: ${offsetRanges.partition}, fromOffset: ${offsetRanges.fromOffset}, untilOffset: ${offsetRanges.untilOffset}, count: ${offsetRanges.count()}
          """.stripMargin)
        val update = kafkaCluster.setConsumerOffsets(groupId, Map(TopicAndPartition(offsetRanges.topic, offsetRanges.partition) -> offsetRanges.untilOffset))
        if (update.isLeft) {
          logger.error(s"update topic '${offsetRanges.toString()}'  encounter error: " + update.left)
        }
      }

    }



    ssc.start()
    ssc.awaitTermination()




  }



}
