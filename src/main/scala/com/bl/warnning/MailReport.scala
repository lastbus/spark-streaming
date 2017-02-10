package com.bl.warnning

/**
  * Created by make on 11/14/16.
  */
sealed trait MailReport {
  val subject: String
  val body: String
}
case class ParserReport(override val subject: String, override val body: String) extends MailReport
case class StatusReport(override val subject: String, override val body: String) extends MailReport
