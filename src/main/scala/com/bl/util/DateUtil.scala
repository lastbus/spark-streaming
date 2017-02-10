package com.bl.util

import java.text.SimpleDateFormat

import scala.collection.mutable

/**
  * Created by make on 11/11/16.
  */
object DateUtil {

  val dateFormatMap = mutable.Map[String, ThreadLocal[SimpleDateFormat]]()


  def transform(source: String, sourceFormat: String, targetFormat: String): String = {

    val sdf1 = getSimpleDateFormat(sourceFormat)
    val sdf2 = getSimpleDateFormat(targetFormat)
    sdf2.format(sdf1.parse(source))

  }

  def getSimpleDateFormat(dateFormat: String): SimpleDateFormat = {
    val sdf = dateFormatMap.get(dateFormat)
    if (sdf.isEmpty) {
      synchronized {
        dateFormatMap(dateFormat) = new ThreadLocal[SimpleDateFormat] {
          override def initialValue(): SimpleDateFormat = {
            new SimpleDateFormat(dateFormat)
          }
        }
      }
    }
    dateFormatMap(dateFormat).get()
  }


}
