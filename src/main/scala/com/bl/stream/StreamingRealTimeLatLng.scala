package com.bl.stream

import java.io.FileNotFoundException
import java.util.Properties

import com.bl.hbase.HBaseConnectionFactory
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json.{JSONException, JSONObject}

import scala.collection.JavaConversions._

/**
  * direct streaming don't remember topic's partition offset,
  * if you don't give it offset, then it will poll the latest records in topics by default.
  * Created by make on 10/31/16.
  */
object StreamingRealTimeLatLng {


  val logger = Logger.getLogger(this.getClass.getName)


  def main(args: Array[String]): Unit = {

    this.getClass.getSimpleName
    if (args.length < 1) {
      printUsage
      sys.exit(-1)
    }

    var topic: String = null
    var seconds = 1


    for (i <- 0 until args.length if i % 2 == 0) {
      args(i) match {
        case "-t" => topic = args(i + 1)
        case "-i" => seconds = args(i + 1).toInt
        case e =>
          printUsage
          throw new Exception("not known option: " + e)
      }
    }

    require(topic != null, "Please subscribe at least on topic")

    val props = getPropertiesMap("kafka.properties")
    val topicSet = topic.split(",").toSet
    val groupId = props.get("group.id")
    logger.info("subscribe topic: " + topic + ",  consumer group id: " + groupId)


    if (groupId.isEmpty) {
      throw new Exception("missing group.id param in kafka.properties file")
    }

    val kafkaCluster = new KafkaCluster(props)

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
            println("consumer group:" + groupId + ",topic:" + tp.topic + ",partition:" + tp.partition +
              " offsets已经过时，更新为" + earliestLeaderOffset)
            offsets += (tp -> earliestLeaderOffset)
          }
        })
        if (!offsets.isEmpty) {
          kafkaCluster.setConsumerOffsets(groupId.get, offsets)
        }
      } else {
        val reset = props.get("auto.offset.reset").map(_.toLowerCase)
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

    /**

    val topicAndPartitionEither = kafkaCluster.getPartitions(topicSet)
    // if cannot find topic set info, then exit.
    if (topicAndPartitionEither.isLeft) {
      throw new Exception("get kafka partition failed: " + topicSet + " , " + topicAndPartitionEither.left)
    }
    val topicAndPartitionSet = topicAndPartitionEither.right.get
    val consumerOffset = kafkaCluster.getConsumerOffsets(groupId.get, topicAndPartitionSet)
    if (consumerOffset.isLeft) {
      logger.info("zookeeper cannot find group offset, initialize consumer offset ")
      // begin to set the group consumer offset, default to the earliest
      val offsetRest = props.getOrElse("auto.offset.reset", "smallest")
      val earliestLeaderOffset =
        if (offsetRest == "smallest")
        kafkaCluster.getEarliestLeaderOffsets(topicAndPartitionSet)
      else if (offsetRest == "largest")
        kafkaCluster.getLatestLeaderOffsets(topicAndPartitionSet)
      else
        kafkaCluster.getEarliestLeaderOffsets(topicAndPartitionSet)
//      val earlistLeaderOffset = kafkaCluster.getEarliestLeaderOffsets(topicAndPartitionSet)

      if (earliestLeaderOffset.isLeft) {
        logger.error("get earliest leader offset encounter error: " + topicAndPartitionSet)
        sys.exit(-1)
      } else {
        kafkaCluster.setConsumerOffsets(groupId.get, earliestLeaderOffset.right.get.mapValues(_.offset))
      }
    }

      */

    val topicAndPartitionSet = kafkaCluster.getPartitions(topicSet).right.get
    val topicAndPartitionMap = kafkaCluster.getConsumerOffsets(groupId.get, topicAndPartitionSet).right.get

    val sparkConf = new SparkConf()
    //sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "1000") this can be set in command line
    val ssc = new StreamingContext(sparkConf, Seconds(seconds))

    val message =  KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, Int, Long, String, String)](
      ssc, props, topicAndPartitionMap, (mm: MessageAndMetadata[String, String]) => (mm.topic, mm.partition, mm.offset, mm.key(), mm.message()))


    var offsetRanges = Array[OffsetRange]()
    val receiveRawBytes = Bytes.toBytes("json")
    val columnFamilyBytes = Bytes.toBytes("info")
    val createDateBytes = Bytes.toBytes("create_date")
    val memberIdBytes = Bytes.toBytes("member_id")
    val memberLantitudeBytes = Bytes.toBytes("member_latitude")
    val memberLongitudeBytes = Bytes.toBytes("member_longitude")
    val methodBytes = Bytes.toBytes("method")
    val methodTypeBytes = Bytes.toBytes("method_type")


    message.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.foreachRDD { rdd =>
      // there are more than one records in a seconds, so zip records with unique id to identify them
      rdd.zipWithUniqueId().foreachPartition { partition =>

        val hTable = HBaseConnectionFactory.getConnection("real_time_lat_lng")
        val puts =
        partition.map { case ((topic, partition, offset, k, v), index) =>
          logger.info(s"topic:$topic, partition: $partition, offset: $offset, key: $k, value: $v")
          try {
            val json = new JSONObject(v)
            val createDate = json.getString("createDate")
            val memberId = json.getString("memberId")
            val memberLantitude = json.getString("memberLantitude")
            val memberLongitude = json.getString("memberLongitude")
            val method = json.getString("method")
            val methodType = json.getString("methodType")
            val put = new Put(Bytes.toBytes(s"$createDate|$memberId|$methodType|$index"))
            put.addColumn(columnFamilyBytes, receiveRawBytes, Bytes.toBytes(v))
            put.addColumn(columnFamilyBytes, createDateBytes, Bytes.toBytes(createDate))
            put.addColumn(columnFamilyBytes, memberIdBytes, Bytes.toBytes(memberId))
            put.addColumn(columnFamilyBytes, memberLantitudeBytes, Bytes.toBytes(memberLantitude))
            put.addColumn(columnFamilyBytes, memberLongitudeBytes, Bytes.toBytes(memberLongitude))
            put.addColumn(columnFamilyBytes, methodBytes, Bytes.toBytes(method))
            put.addColumn(columnFamilyBytes, methodTypeBytes, Bytes.toBytes(methodType))
            Some(put)
          } catch {
            case e: JSONException =>
              logger.error("JSONException: " + e)
              None
            case e: Throwable => throw e
          }

        }.toList.flatMap(s=>s)//.filter(_.isDefined).map(_.get).toList
        try {
          hTable.put(puts)
          // commit result ???
          // TODO commit result,
//          hTable.close()
        } catch {
          case e: Throwable => throw e
        }

      }

      // this code block is executing on local machine
      offsetRanges.foreach { offsetRanges =>
        logger.debug(
          s"""
             |topic: ${offsetRanges.topic}, partition: ${offsetRanges.partition}, fromOffset: ${offsetRanges.fromOffset}, untilOffset: ${offsetRanges.untilOffset}, count: ${offsetRanges.count()}
          """.stripMargin)

        val update = kafkaCluster.setConsumerOffsets(groupId.get, Map(TopicAndPartition(offsetRanges.topic, offsetRanges.partition) -> offsetRanges.untilOffset))
        if (update.isLeft) {
          logger.error(s"update topic '${offsetRanges.toString()}' encounter error: " + update.left)
        }
      }

    }



    ssc.start()
    ssc.awaitTermination()


  }

  def printUsage = {
    println(
      s"""
         |error parameters
         |Usage: ${this.getClass.getSimpleName}  -t <topics> -i <interval>
         |  -t  subscribe topics, topic1, topic2,...
         |  -i  spark streaming interval, default 1s
         |
        """.stripMargin)
  }

  def initialTopicsOffset(params: Map[String, String]): Unit = {

  }



  def getPropertiesMap(fileName: String): Map[String, String] = {
    // load properties file
    val inputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream(fileName)

    if (inputStream == null) {
      throw new FileNotFoundException(s"Cannot found ${fileName} file, please make sure this file is in your classpath.")
    }

    val props = new Properties()
    props.load(inputStream)
    inputStream.close()
    logger.info(s"-------  ${fileName}  --------")
    val properties = for ((k, v) <- props) yield {
      logger.info(k + ":" + v)
      (k, v)
    }
    logger.info("----------------------")
    // kafka client properties
    properties.toMap
  }




}
