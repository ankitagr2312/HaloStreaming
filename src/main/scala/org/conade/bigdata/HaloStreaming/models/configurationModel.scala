package org.conade.bigdata.HaloStreaming.models

case class configurationModel(
    appName: String,
    sparkProperties: Map[String, String],
    dataProcessor:ProcessorModel,
    inputProperties: IOModel,
    inputSchema : List[(String,String)],
    writerProperties: IOModel
)
