package com.bl.warnning

import com.bl.util.FileUtil
import org.apache.commons.mail.{DefaultAuthenticator, SimpleEmail}
import org.apache.log4j.LogManager

import scala.collection.mutable.ArrayBuffer

/**
  * Created by make on 11/13/16.
  */
object MailUtil {

  val logger = LogManager.getLogger(this.getClass)
  var statusLastSendTime = 0L
  var parserReportLastTime = 0L
  val mailReportList = new ArrayBuffer[MailReport]()


  /**
    * 发送报警邮件
    * @param mailReport
    */
  def sendMail(mailReport: MailReport): Unit = {
    mailReport match {
      case msg1: StatusReport => StatusReportUtil.send(msg1)
      case msg2: ParserReport => ParseReportUtil.send(msg2)
    }
  }


//  def sendMail(mailReport: MailReport): Unit = {
//
//    val now = System.currentTimeMillis()
//    // send mail interval must be larger than five minutes
//    mailReport match {
//      case StatusReport(subject, body) =>
//        if ((now - statusLastSendTime) > 60 * 1000) {
//          val mail = getMail()
//          mail.setSubject(mailReport.subject)
//          mail.setMsg(mailReport.body)
//          val sendMailThread = new Thread(new Runnable {
//            override def run(): Unit = {
//              try {
//                mail.send()
//                statusLastSendTime = now
//              } catch {
//                case e =>
//                  logger.error("send mail error: " + e)
//                  throw e
//              }
//            }
//          })
//          sendMailThread.start()
//        }
//      case ParserReport(subject, body) =>
//        if ((now - parserReportLastTime) > 10 * 60 * 1000) {
//          val mail = getMail()
//          mail.setSubject(mailReport.subject)
//          mail.setMsg(mailReport.body)
//          val sendMailThread = new Thread(new Runnable {
//            override def run(): Unit = {
//              try {
//                mail.send()
//                parserReportLastTime = now
//              } catch {
//                case e =>
//                  logger.error("send mail error: " + e)
//                  throw e
//              }
//            }
//          })
//          sendMailThread.start()
//        }
//    }
//
//  }


  /**
    * 得到发送邮件类
    * @param mailType 邮件类型
    * @return SimpleEmail
    */
  private[warnning] def getMail(mailType: String = "default"): SimpleEmail = {

    val email = new SimpleEmail()
    val map = FileUtil.getPropertiesMap("mail.properties")

    email.setHostName(map(s"mail.host.${mailType}"))
    email.setAuthenticator(new DefaultAuthenticator(map(s"mail.user.${mailType}"), map(s"mail.password.${mailType}")))
    email.setFrom(map(s"mail.from.${mailType}"))
    val to = map(s"mail.to.${mailType}").split(",")
    to.foreach(email.addTo(_))
    email

  }



  def main(args: Array[String]): Unit = {

    sendMail(StatusReport("status-error", "test"))
    sendMail(ParserReport("parse-error", "test"))

  }

}
