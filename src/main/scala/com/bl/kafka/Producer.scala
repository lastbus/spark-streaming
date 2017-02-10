package com.bl.kafka

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.json.JSONObject

import scala.util.Random

/**
  * this is for test
  * Created by make on 10/31/16.
  */
object Producer {
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "0")
    props.put("retries", "0")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("buffer.memoery", "33554432")
    val kafkaProducer = new KafkaProducer[String, String](props)
    var i = 0


    while (true) {

      val json = getMemberString()

      kafkaProducer.send(new ProducerRecord[String, String]("memberTopic", i.toString, json.toString),
        new Callback {
          override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
            if (e != null) println("Error: " + e)
            else
            println (
              s"""
                |topic: ${recordMetadata.topic()}, partition: ${recordMetadata.partition()}, offset: ${recordMetadata.offset()}, record: $json
              """.stripMargin)
            println("send msg: " + i)
          }
        })
      Thread.sleep(20)
      i += 1
    }








  }

  val random = new Random()

  def getMemberString() : String = {
    val id = random.nextLong()
      s"""
        |{"fireFirstRegEventRequired":false,"fireBindCard":false,"fireShangHaiBankPromotion":false,"firstRealName":false,"memberId":${id},"mobile":"18621878101","loginPasswd":"afbd90961354d83675795610731c7d59","birthYear":0,"birthMonth":0,"birthDay":0,"birtyLunarMonth":0,"birthLunarDay":0,"registerTime":"Jan 18, 2017 5:10:12 PM","channelId":"2","orgId":"3000","familyMemberNum":0,"childRenNum":0,"memberStatus":"0","createTime":"Jan 18, 2017 5:10:12 PM","random":"4610","age":0,"blackFlag":0,"mobileBindFlag":1,"memberLevel":10,"getNums":0,"backNums":0,"isApplyCancle":"0","idFlag":"0","fakePeopleFlag":0}
      """.stripMargin
  }


  def getJSON(i: Int) : JSONObject = {

    val json = new JSONObject()
    json.put("createDate", sdf.format(new Date()))
    json.put("memberId", i.toString)
    json.put("memberLantitude", "31.177778")
    json.put("memberLongitude", "121.501247")
    json.put("method", "NsupplyerController.range")
    json.put("methodType", (i % 4 + 1).toString)
    json

  }


}
