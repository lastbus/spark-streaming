package com.bl

import com.bl.util.URLMatcher
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.junit.Test
import scala.collection.JavaConversions._


/**
  * Created by make on 11/14/16.
  */

@Test
class URLSelectTest {

  @Test
  def test = {
    val urls = Array("/recommend/rec?api=cart&memberId=100000001783152&items=349962,215749,120883,354582,166370,167137,182775,232797,311428,254288,74805,303222,229330,69762,220807,298538,298577&chan=3&pNum=1&pSize=16",
    "/recommend/pcgl?mId=100000000820284",
    "")

    val recRegex = "/recommend/rec\\?*(.*)".r
    val pcglRegex = "/recommend/pcgl.*".r



    "/recommend/rec12" match {
      case recRegex(a) =>
        println(a.length)
        println("rec regex: " + a)

      case pcglRegex => println("pcgl regex: " + pcglRegex)
      case e => println("no match: " + e)
    }

  }

  @Test
  def testURLMatcher = {

    val testUrl = Array(
      "/recommend/appgl?pSize=10&pNum=1",
      "/recommend/view?gId=1166029&cookieId=79828827728214775491877",
      "/recommend/dpgl?chan=1&mId=100000001618588&gId=66523",
      "/recommend/appgl?pSize=10&pNum=1",
      "/recommend/appgl?pSize=10&pNum=1&mId=100000001203484&isColl=1",
      "/recommend/serwd?pageType=index&chan=3&num=10",
      "/recommend/rec?api=cart&memberId=100000001783152&items=349962,215749,120883,354582,166370,167137,182775,232797,311428,254288,74805,303222,229330,69762,220807,298538,298577&chan=3&pNum=1&pSize=16",
      "/favicon.ico",
      "/manager/images/tomcat.gif")

    for (i <- 0 until testUrl.length) {
      println(testUrl(i))
      val put = new Put(Bytes.toBytes(i))
      URLMatcher.parse(testUrl(i), put)
      val cells = put.cellScanner()
      while (cells.advance()) {
        val cell = cells.current()
        println(Bytes.toString(cell.getFamily) + ":" + Bytes.toString(cell.getQualifier) + "\t" + Bytes.toString(cell.getValue))
      }

    }




  }

}
