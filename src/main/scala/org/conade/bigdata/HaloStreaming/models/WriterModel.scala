package org.conade.bigdata.HaloStreaming.models

case class IOModel(
    writer: String,
    confProperties: Map[String, String],
    metaData :Option[Map[String, String]]
)
