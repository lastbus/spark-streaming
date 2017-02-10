package com.bl

import com.bl.member.MemberTopicParser
import org.apache.hadoop.hbase.util.Bytes
import org.json.JSONObject
import org.junit.Test

import scala.collection.JavaConversions._

/**
  * Created by make on 1/18/17.
  */

@Test
class JSONTest {

  @Test
  def memberTest = {

    val s =
      """
        |{"fireFirstRegEventRequired":false,"fireBindCard":false,"fireShangHaiBankPromotion":false,"firstRealName":false,"memberId":100000003508167,"mobile":"18621878101","loginPasswd":"afbd90961354d83675795610731c7d59","birthYear":0,"birthMonth":0,"birthDay":0,"birtyLunarMonth":0,"birthLunarDay":0,"registerTime":"Jan 18, 2017 5:10:12 PM","channelId":"2","orgId":"3000","familyMemberNum":0,"childRenNum":0,"memberStatus":"0","createTime":"Jan 18, 2017 5:10:12 PM","random":"4610","age":0,"blackFlag":0,"mobileBindFlag":1,"memberLevel":10,"getNums":0,"backNums":0,"isApplyCancle":"0","idFlag":"0","fakePeopleFlag":0}
      """.stripMargin

    val json = new JSONObject(s)
    MemberTopicParser.init()
    MemberTopicParser.parse(s)

    println(json.getLong("memberId"))
    println(json.getInt("memberId"))

    json.keySet().foreach { key =>
      val jv = json.get(key)

      if (jv.isInstanceOf[String]) {
        println(key + " : " + jv.asInstanceOf[String])

      } else if (jv.isInstanceOf[Int]) {
        println(key + " : " + jv.asInstanceOf[Int])
      } else if (jv.isInstanceOf[Long]) {
        println(key + " : " + jv.asInstanceOf[Long])
      } else if (jv.isInstanceOf[Double]) {
        println(key + " : " + jv.asInstanceOf[Double])
      } else if (jv.isInstanceOf[Boolean]) {
        println(key + " : " + jv.asInstanceOf[Boolean])
      } else {
        //TODO send mail

      }

    }


  }

}
