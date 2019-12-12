package org.conade.bigdata.HaloStreaming.util

import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, sql}
import org.conade.bigdata.HaloStreaming.models.configurationModel

object ConfSetter {

  def getSparkSession(confModel: configurationModel): SparkSession = {
    val conf = new SparkConf()
    confModel.sparkProperties.foreach(prop => conf.set(prop._1, prop._2))

    SparkSession
      .builder()
      .config(conf)
      .appName(confModel.appName)
      .getOrCreate()
  }

  def getInputWriter(spark: SparkSession,
                          confModel: configurationModel): sql.DataFrame = {
    val reader = confModel.inputProperties.writer
    reader match {
      case "kafka" => {
        val streamer = spark.readStream.format("kafka")
        confModel.inputProperties.confProperties.foreach(prop =>
          streamer.option(prop._1, prop._2))
        streamer.load()
      }
      case "socket" => {
        val streamer = spark.readStream.format("socket")
        confModel.inputProperties.confProperties.foreach(prop =>
          streamer.option(prop._1, prop._2))
        streamer.load()
      }
      case "csv" => {
        val streamer = spark.readStream.format("csv")
        confModel.inputProperties.confProperties.foreach(prop =>
          streamer.option(prop._1, prop._2))
        val path =  confModel.inputProperties.metaData match {
          case Some(m) => m.get("path").toString
          case None => throw new java.lang.RuntimeException("Path was not found for file as reader")
        }
        streamer.load(path)
      }
    }
    val streamer = spark.readStream.format("kafka")
    confModel.inputProperties.confProperties.foreach(prop =>
      streamer.option(prop._1, prop._2))
    streamer.load()
  }

  def getWriter(spark: SparkSession,
                           confModel: configurationModel,
                           df: sql.DataFrame): DataStreamWriter[Row] = {
    val writer = confModel.writerProperties.writer
    writer match {
      case "kafka" => {
        val streamer = df.writeStream
          .format("kafka")
        confModel.writerProperties.confProperties.foreach(prop =>
          streamer.option(prop._1, prop._2))
        streamer
      }
      case "parquet" => {
        val streamer = df.writeStream
          .format("parquet")
        confModel.writerProperties.confProperties.foreach(prop =>
          streamer.option(prop._1, prop._2))
        streamer
      }
      case "console" => {
        val streamer = df.writeStream
          .format("console")
        confModel.writerProperties.confProperties.foreach(prop =>
          streamer.option(prop._1, prop._2))
        streamer
      }
      case _ => throw new Exception("Please specify either kafka or parquet")
    }
  }
}
