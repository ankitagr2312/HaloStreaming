package org.conade.bigdata.HaloStreaming.util

import org.conade.bigdata.HaloStreaming.models.configurationModel
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scala.io.Source

object FileReaderAndMapper extends java.io.Serializable {

  def getJobConfigurationModel(jsonPath: String): configurationModel = {

    implicit val formats = DefaultFormats
    val jsonConfFile = Source.fromFile(jsonPath).getLines.mkString
    parse(jsonConfFile).extract[configurationModel]
  }
}
