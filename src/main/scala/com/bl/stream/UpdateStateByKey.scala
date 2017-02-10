package com.bl.stream

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by make on 9/27/16.
  */
object UpdateStateByKey {

  def main(args: Array[String]): Unit = {

//    Logger.getLogger("org").setLevel(Level.WARN)

    val ssc = new StreamingContext("local[2]", "updateStateByKey study", Seconds(3))
    ssc.checkpoint("/tmp/spark/checkpoint")

    val lines = ssc.socketTextStream("localhost", 9999)

    val wordPair = lines.flatMap(_.split(" ")).map((_, 1))

    val wordCount = wordPair.updateStateByKey[Int](updateFunc _)
    wordCount.print()

    ssc.start()
    ssc.awaitTermination()


  }

  def updateFunc(newValue: Seq[Int], runningValue: Option[Int]): Option[Int] = {
    Some(newValue.sum + runningValue.getOrElse(0))
  }

}
