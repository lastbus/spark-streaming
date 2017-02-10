package com.bl.flume

import java.io.{BufferedReader, InputStreamReader}
import java.net.{HttpURLConnection, URL}


/**
  * Created by make on 11/11/16.
  */
object FileGrowthTest {

  def main(args: Array[String]): Unit = {

    val requestNum = 100
    for (i <- 0 until requestNum) {
//      val start = System.currentTimeMillis()
      request(1)
  //    println(System.currentTimeMillis() - start)
//      println("Concurrent :  " + requestNum.toDouble / (System.currentTimeMillis() - start) * 1000 )
      Thread.sleep(1000)
    }



  }

  // 构造 n 个兵法请求
  def request(requestNum: Int = 1): Unit = {

    for (i <- 0 until requestNum) {

      val request = new Thread(new Runnable {
        override def run(): Unit = {
          val url = new URL("http://localhost:8080")
          val conn = url.openConnection().asInstanceOf[HttpURLConnection]
          val in = conn.getInputStream
          val reader = new BufferedReader(new InputStreamReader(in))
          val sb = new StringBuffer()
          var line = reader.readLine()
          while (line != null) {
            sb.append(line)
            line = reader.readLine()
          }
//          println(sb)
          in.close()
          conn.disconnect()
        }
      })

      request.start()
    }


  }


}
