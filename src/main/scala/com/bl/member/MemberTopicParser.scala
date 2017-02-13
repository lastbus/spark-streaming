package com.bl.member

import com.bl.util.FileUtil
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.LogManager
import org.json.JSONObject

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by make on 1/18/17.
  */
object MemberTopicParser extends JSONParser {

  val logger = LogManager.getLogger(this.getClass)

  val keyTypeMap = mutable.Map.empty[String, (String, Array[Byte])]
  val columnFamilyBytes = Bytes.toBytes("member")

  /**
    * 初始化解析器
    */
  def init(): Unit = {
    val properties = FileUtil.getPropertiesMap("memberJSON.properties")

    properties.foreach { case (v1, v2) =>
        val s = v2.split(":")
      if (s.startsWith("NUMBER")) {
          keyTypeMap.put(v1.toLowerCase(), ("long", Bytes.toBytes(v1.toLowerCase())))
        }  else if (s.startsWith("VARCHAR2")) {
      keyTypeMap.put(v1.toLowerCase(), ("string", Bytes.toBytes(v1.toLowerCase())))
      }

    }
  }

  /**
    * 解析 json 字符串
    * @param jsonString
    * @return
    */
  override def parse(jsonString: String): Put = {
    logger.warn(jsonString)

    val json = new JSONObject(jsonString)
    val memberId = json.get("memberId").toString
    val put = new Put(Bytes.toBytes(DigestUtils.md5Hex(memberId)))
    logger.debug(DigestUtils.md5Hex(memberId))

    json.keys().foreach { key =>
      logger.debug(key)

      val v = keyTypeMap.get(key.toLowerCase)

      if (v.isDefined) {
        v.get._1 match {
          case "string" => put.addColumn(columnFamilyBytes, v.get._2, Bytes.toBytes(json.getString(key)))
          case "int" => put.addColumn(columnFamilyBytes, v.get._2, Bytes.toBytes(json.getInt(key)))
          case "double" => put.addColumn(columnFamilyBytes, v.get._2, Bytes.toBytes(json.getDouble(key)))
          case "long" => put.addColumn(columnFamilyBytes, v.get._2, Bytes.toBytes(json.getLong(key)))
          case "boolean" => put.addColumn(columnFamilyBytes, v.get._2, Bytes.toBytes(json.getBoolean(key)))
          case e => put.addColumn(columnFamilyBytes, v.get._2, Bytes.toBytes(json.getString(key)))
        }
      } else {
        val jv = json.get(key)
        logger.debug(jv)
        if (jv.isInstanceOf[String]) {
          put.addColumn(columnFamilyBytes, Bytes.toBytes(key), Bytes.toBytes(jv.asInstanceOf[String]))
          keyTypeMap.put(key.toLowerCase(), ("string", Bytes.toBytes(key)))
        } else if (jv.isInstanceOf[Int]) {
          put.addColumn(columnFamilyBytes, Bytes.toBytes(key), Bytes.toBytes(jv.asInstanceOf[Int]))
          keyTypeMap.put(key, ("int", Bytes.toBytes(key)))
        } else if (jv.isInstanceOf[Long]) {
          put.addColumn(columnFamilyBytes, Bytes.toBytes(key), Bytes.toBytes(jv.asInstanceOf[Long]))
          keyTypeMap.put(key, ("long", Bytes.toBytes(key)))
        } else if (jv.isInstanceOf[Double]) {
          put.addColumn(columnFamilyBytes, Bytes.toBytes(key), Bytes.toBytes(jv.asInstanceOf[Double]))
          keyTypeMap.put(key, ("double", Bytes.toBytes(key)))
        } else if (jv.isInstanceOf[Boolean]) {
          put.addColumn(columnFamilyBytes, Bytes.toBytes(key), Bytes.toBytes(jv.asInstanceOf[Boolean]))
          keyTypeMap.put(key, ("boolean", Bytes.toBytes(key)))
        } else {
          //TODO send mail
          logger.error("don't know type: " + key + " : " + jv)
        }


      }
    }

    put

  }




}
