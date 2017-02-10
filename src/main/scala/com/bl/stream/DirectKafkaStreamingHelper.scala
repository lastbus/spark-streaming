package com.bl.stream

import kafka.common.TopicAndPartition
import org.apache.log4j.LogManager
import org.apache.spark.SparkException
import org.apache.spark.streaming.kafka.KafkaCluster
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset

/**
  * 封装 spark streaming 从 kafka 中读取数据
  * Created by make on 11/11/16.
  */
object DirectKafkaStreamingHelper {

  val logger = LogManager.getLogger(this.getClass)


  /** 检查输入 topic 的合法性 */
  def prepare(topicSet: Set[String], kafkaCluster: KafkaCluster): Map[TopicAndPartition, Long] = {

    val properties = kafkaCluster.kafkaParams
    val groupId = properties.get("group.id")
    require(groupId.isDefined, "please input group.id")

    topicSet.foreach(topic => {
      var hasConsumed = true
      val partitionsE = kafkaCluster.getPartitions(Set(topic))
      if (partitionsE.isLeft){
        throw new SparkException(s"get kafka partition failed: ${partitionsE.left.get}")
      }
      val partitions = partitionsE.right.get
      val consumerOffsetsE = kafkaCluster.getConsumerOffsets(groupId.get, partitions)
      if (consumerOffsetsE.isLeft) {
        hasConsumed = false
      }
      if (hasConsumed) {
        /**
          * 如果streaming程序执行的时候出现kafka.common.OffsetOutOfRangeException，
          * 说明zk上保存的offsets已经过时了，即kafka的定时清理策略已经将包含该offsets的文件删除。
          * 针对这种情况，只要判断一下zk上的consumerOffsets和earliestLeaderOffsets的大小，
          * 如果consumerOffsets比earliestLeaderOffsets还小的话，说明consumerOffsets已过时,
          * 这时把consumerOffsets更新为earliestLeaderOffsets
          */
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
            logger.info("consumer group:" + groupId + ",topic:" + tp.topic + ",partition:" + tp.partition +
              " offsets已经过时，更新为" + earliestLeaderOffset)
            offsets += (tp -> earliestLeaderOffset)
          }
        })
        if (!offsets.isEmpty) {
          kafkaCluster.setConsumerOffsets(groupId.get, offsets)
        }
      } else {
        val reset = properties.get("auto.offset.reset").map(_.toLowerCase)
        var leaderOffsets: Map[TopicAndPartition, LeaderOffset] = null
        if (reset == Some("smallest")) {
          val leaderOffsetsE = kafkaCluster.getEarliestLeaderOffsets(partitions)
          if (leaderOffsetsE.isLeft)
            throw new SparkException(s"get earliest leader offsets failed: ${leaderOffsetsE.left.get}")
          leaderOffsets = leaderOffsetsE.right.get
        } else {
          val leaderOffsetsE = kafkaCluster.getLatestLeaderOffsets(partitions)
          if (leaderOffsetsE.isLeft)
            throw new SparkException(s"get latest leader offsets failed: ${leaderOffsetsE.left.get}")
          leaderOffsets = leaderOffsetsE.right.get
        }
        val offsets = leaderOffsets.map {
          case (tp, offset) => (tp, offset.offset)
        }
        kafkaCluster.setConsumerOffsets(groupId.get, offsets)
      }


    })
    // 初始化完毕，记录下各个 topic 的 offset
    val partition = kafkaCluster.getPartitions(topicSet).right.get
    val consumerOffset = kafkaCluster.getConsumerOffsets(groupId.get, partition).right.get

    logger.info(" ------------------------------------  ")
    consumerOffset.foreach { case (topicAndPartition, offset) =>
      logger.info(s"topic: ${topicAndPartition.topic}, partition: ${topicAndPartition.partition}, offset: ${offset}")
    }
    logger.info(" ------------------------------------  ")

    consumerOffset
  }

}
