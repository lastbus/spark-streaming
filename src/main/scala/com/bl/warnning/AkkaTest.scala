package com.bl.warnning


import scala.concurrent.duration.Duration

/**
  * Created by make on 11/15/16.
  */
object AkkaTest {






}






sealed trait PiMessage
case object Calculate extends PiMessage
case class Work(start: Int, nrOfElements: Int) extends PiMessage
case class Result(value: Double) extends PiMessage
case class PiApproximation(pi: Double, duration: Duration)
