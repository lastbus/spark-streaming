package com.bl.stream

import com.bl.hbase.HBaseConnectionFactory
import com.bl.util._
import com.bl.warnning.{MailUtil, ParserReport, StatusReport}
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.LogManager
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}

/**
  * Created by make on 11/11/16.
  */
object TomcatAccessLogConsumerStream {

  val logger = LogManager.getLogger(this.getClass)


  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      println("please input consume kafka topic")
      printUsage
      sys.exit(-1)
    }

    // kafka topic
    var topic: String = null
    // spark streaming  interval
    var interval = 1

    // parse command line
    for (i <- 0 until args.length if i % 2 == 0) {
      args(i) match {
        case "-t" => topic = args(i + 1)
        case "-i" => interval = args(i + 1).toInt
        case e =>
          printUsage
          throw new Exception("not known option: " + e)
      }
    }

    require(topic != null, "kafka topic cannot be null")

    // put kafka properties in file: kafka.properties
    val props = FileUtil.getPropertiesMap("kafka.properties")
    // spark streaming consume topic set
    val topicSet = topic.split(",").toSet
    // spark streaming consume group id
    val groupId = props.get("group.id")
    logger.info("subscribe topic: " + topic + ",  consumer group id: " + groupId)

    // connection to kafka cluster
    val kafkaCluster = new KafkaCluster(props)
    // initial topics and corresponding partitions belong to the streaming consume group
    val topicAndPartition = DirectKafkaStreamingHelper.prepare(topicSet, kafkaCluster)

    val sc = new SparkContext(new SparkConf())
    val ssc = new StreamingContext(sc, Seconds(interval))

    val kafkaDirectStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, MessageAndMetadata[String, String]](
      ssc, props, topicAndPartition, (mm: MessageAndMetadata[String, String]) => mm)


    var offsetRange = Array[OffsetRange]()

    val hbaseBrd = sc.broadcast(HBaseTableTomcatAccessLog)

    kafkaDirectStream.foreachRDD { rdd =>
      rdd.foreachPartition { partition =>

        val hTable = HBaseConnectionFactory.getConnection("tomcat_access_log")
        val tmp = hbaseBrd.value

        partition.foreach { messageAndMetaData =>
          var put: Put = null
          try {
            val accessLog = messageAndMetaData.message()
            val host = accessLog.substring(0, accessLog.indexOf("-")).trim
            val rawTime = accessLog.substring(accessLog.indexOf("[") + 1, accessLog.indexOf("]"))
            val time = DateUtil.transform(rawTime, "dd/MMM/yyyy:HH:mm:ss Z", "yyyy-MM-dd HH:mm:ss")

            put = new Put(Bytes.toBytes(time + "|" + messageAndMetaData.partition + "|" + messageAndMetaData.offset))
            put.addColumn(tmp.columnFamilyBytes, tmp.rawData, Bytes.toBytes(messageAndMetaData.message()))
            put.addColumn(tmp.columnFamilyBytes, tmp.hostBytes, Bytes.toBytes(host))
            put.addColumn(tmp.columnFamilyBytes, tmp.dt, Bytes.toBytes(time.substring(0, 10)))

            val lastQ = accessLog.lastIndexOf("\"")
            val rawRequest = accessLog.substring(accessLog.indexOf("\"") + 1, lastQ)
            val methodAndRequest = rawRequest.split(" ")

            put.addColumn(tmp.columnFamilyBytes, tmp.method, Bytes.toBytes(methodAndRequest(0)))

            put.addColumn(tmp.columnFamilyBytes, tmp.uri, Bytes.toBytes(methodAndRequest(1)))
            // parse request key-value pairs
            URLMatcher.parse(methodAndRequest(1), put)
//            val Array(space, app, req) = methodAndRequest(1).split("/")
//            put.addColumn(tmp.columnFamilyBytes, tmp.app, Bytes.toBytes(app))
//            val kvs = req.split("\\?")
//            if (kvs.length > 0) {
//              put.addColumn(tmp.columnFamilyBytes, tmp.interface, Bytes.toBytes(kvs(0)))
//            }
//            if (kvs.length > 1) {
//              kvs(1).split("&").foreach { kv =>
//                val pair = kv.split("=")
//                if (pair.length == 1) {
//                  put.addColumn(tmp.columnFamilyBytes, Bytes.toBytes(pair(0)), Bytes.toBytes(""))
//                } else {
//                  put.addColumn(tmp.columnFamilyBytes, Bytes.toBytes(pair(0)), Bytes.toBytes(pair(1)))
//                }
//              }
//            }
            put.addColumn(tmp.columnFamilyBytes, tmp.protocol, Bytes.toBytes(methodAndRequest(2)))

            // 之前的只有两个参数，后来加上三个
            val array = accessLog.substring(lastQ + 1).trim.split(" ")
            put.addColumn(tmp.columnFamilyBytes, tmp.statusBytes, Bytes.toBytes(array(0)))
            if (array(0) != "200") {
              // if response is not 200, send mail
              logger.error("error response: " + messageAndMetaData.message())
              MailUtil.sendMail(StatusReport("status-error", messageAndMetaData.message()))
            }
            if (array.length > 1) {
              put.addColumn(tmp.columnFamilyBytes, tmp.byte, Bytes.toBytes(array(1)))
            }
            if (array.length > 2) {
              put.addColumn(tmp.columnFamilyBytes, tmp.timeTaken, Bytes.toBytes(array(2)))
            }
            //TODO Exact-once
            hTable.put(put)
            kafkaCluster.setConsumerOffsets(groupId.get, Map(TopicAndPartition(messageAndMetaData.topic, messageAndMetaData.partition) -> messageAndMetaData.offset))
          } catch {
            case e: Exception =>
              //TODO 记录在另一个日志里, 累计到一定程度则发邮件报警
              val msg =
                s"""
                  |topic: ${messageAndMetaData.topic}
                  |message: ${messageAndMetaData.message()}
                  |exception: $e
                """.stripMargin
              logger.error("topic: " + messageAndMetaData.topic + ", message: " + messageAndMetaData.message() + ": " + e)
              MailUtil.sendMail(ParserReport("parse-error", msg))
          }


        }



      }

    }


    ssc.start()
    ssc.awaitTermination()



  }

  /**
    * 输出用法
    */
  def printUsage = {
    println(
      s"""
        | 用于消费 kafka 中 tomcat 请求处理日志数据，处理完毕保存到 HBase 中。
        |Usage: ${this.getClass.getSimpleName}  -t <topic>  [-i <interval>]
        |       -t <topic> kafka topic to be consumer, must have
        |       -i <interval>, unit second, default  1 s
        |
      """.stripMargin)
  }

}
