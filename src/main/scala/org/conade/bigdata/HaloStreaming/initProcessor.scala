package org.conade.bigdata.HaloStreaming

import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.Trigger
import org.conade.bigdata.HaloStreaming.processor.DataExtraction
import org.conade.bigdata.HaloStreaming.util.{ConfSetter, FileReaderAndMapper}

/**
 * This class has mojor four functions
 * 1.Creating spark configuration model
 * 2.Reading data from input as it is.
 * 3.Extracting fields that are required from data read.
 * 4.Wrting that data to the target Specified.
 *
 * @arg is the configuration json that is build in pattern of configuration model
 *
 * */
object initProcessor {
  def main(args: Array[String]): Unit = {

    //Setting configuration for spark after parsing configuration file
    val confModel = FileReaderAndMapper.getJobConfigurationModel(args(0))
    val spark = ConfSetter.getSparkSession(confModel)
    //Reading data from input source
    val df = ConfSetter.getInputWriter(spark, confModel)
    //extraction of fields required in target
    val processedData = DataExtraction(confModel).extractData(df,spark)
    //Writing extracted data to target.
    val writer = ConfSetter
      .getWriter(spark, confModel, processedData)
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    writer.awaitTermination()
  }
}
