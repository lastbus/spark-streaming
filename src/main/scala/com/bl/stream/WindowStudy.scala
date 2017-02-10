package com.bl.stream

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by make on 9/27/16.
  */
object WindowStudy {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
//    Logger
    val ssc = new StreamingContext("local[2]", "window study", Seconds(5))
    ssc.checkpoint("/tmp/spark/checkpoint")

    val lines = ssc.socketTextStream("localhost", 9999)

    val words = lines.flatMap(_.split(" "))
    val pairs = words.map((_, 1))
    val wordCount = pairs.reduceByKey(_ + _)
    wordCount.print

    ssc.start()
    ssc.awaitTermination()


  }

}
