package com.bl.member

/**
  * Created by make on 1/18/17.
  */
object JSONParserFactory {


  def getParser(name: String): JSONParser = {

    name match {
      case "memberTopic" => MemberTopicParser
      case e => null
    }

  }

}
