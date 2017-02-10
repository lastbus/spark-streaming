package com.bl.warnning

import com.bl.util.FileUtil
import org.apache.log4j.LogManager

import scala.collection.mutable.ArrayBuffer

/**
  * Created by make on 11/15/16.
  */
object ParseReportUtil {

  val logger = LogManager.getLogger(this.getClass)

  var send = false
  var failed = false
  val max = 200
  val mailReportList = new ArrayBuffer[MailReport](max)

  val interval = FileUtil.getPropertiesMap("warning.properties").getOrElse("warning.parse.interval", "300").toInt

  // send e-mail for error http response status
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

  // a single thread to send mail
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

  def sendMail(mailReports: Seq[MailReport]): Unit = {
    val mail = MailUtil.getMail()
    mail.setSubject(mailReports.head.subject)
    mail.setMsg(mailReports.mkString("\n"))
    mail.send()
  }



}
