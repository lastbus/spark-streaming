package com.bl

import java.text.SimpleDateFormat
import java.util.Date


import com.bl.util.DateUtil


/**
 * Hello world!
 *
 */
object App  {

  def main(args: Array[String]): Unit = {

    val accessLog = "10.201.48.10 - - [11/Nov/2016:20:25:40 +0800] \"GET /recommend/appgl?pSize=10&pNum=1&mId=100000000003315&isColl=1 HTTP/1.0\" 200 2143"


    val host = accessLog.substring(0, accessLog.indexOf("-")).trim
    println(host)

    val time = accessLog.substring(accessLog.indexOf("[") + 1, accessLog.indexOf("]"))
    println(time)

    println(DateUtil.transform(time, "dd/MMM/yyyy:HH:mm:ss Z", "yyyy-MM-dd HH:mm:ss"))


    val lastQ = accessLog.lastIndexOf("\"")
    val Array(m, n, q) = accessLog.substring(accessLog.indexOf("\"") + 1, lastQ).split(" ")
    println(m + " " + q )

    val Array(space, app, kv) = n.split("/")
    println(space)
    println(app)
    println(kv)
    val kvs = kv.split("\\?")
    println(kvs(0))
    if (kvs.length > 1) {
      kvs(1).split("&").foreach(println _)
    }


    val a = accessLog.substring(lastQ + 1).trim


    println(a)



  }
}

//class TestActor extends Actor {
//
//  override def receive: Receive = {
//
//  }
//}
