package com.bl.warnning

import com.bl.util.FileUtil
import org.apache.log4j.LogManager

import scala.collection.mutable.ArrayBuffer

/**
  * Created by make on 11/15/16.
  */
object StatusReportUtil {

  val logger = LogManager.getLogger(this.getClass)

  // 是否发送
  var send = false
  // 发送是否成功
  var failed = false
  // 错误消息队列的最大数量
  val max = 100
  // 错误信息队列
  val mailReportList = new ArrayBuffer[MailReport](max)
  // 发送错误信息的时间间隔
  val interval = FileUtil.getPropertiesMap("warning.properties").getOrElse("warning.status.interval", "30").toInt

  /**
    * 发送 tomcat 出错的http请求
    * @param mailReport
    */
  def send(mailReport: MailReport): Unit = {
    mailReportList.append(mailReport)

    if (!send) {
      sendThread
      send = true
      failed = false
    } else if (failed) {
      sendThread
      send = true
      failed = false
    }

  }

  /**
    * 发送邮件的线程，异步
    */
  def sendThread = {
    val sendThread = new Thread(new Runnable {
      override def run(): Unit = {
        Thread.sleep(interval * 1000)
        logger.info("prepare send mail")
        try {
          sendMail(mailReportList)
          mailReportList.clear()
          send = false
          failed = false
          logger.info("send mail success !")
        } catch {
          case e =>
            logger.error("send mail failed: " + e)
            // just keep latest 100 records
            if (mailReportList.length > max) {
              mailReportList.remove(0, mailReportList.length - max)
            }
            send = true
            failed = true
        }
      }
    })
    sendThread.start()
  }

  /**
    * 发送邮件
    * @param mailReports
    */
  def sendMail(mailReports: Seq[MailReport]): Unit = {
    val mail = MailUtil.getMail()
    mail.setSubject(mailReports.head.subject)
    mail.setMsg(mailReports.mkString("\n"))
    mail.send()
  }


}
