package org.conade.bigdata.HaloStreaming.util

import org.apache.spark.sql.SparkSession

object JsonReader {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Reader")
      .master("local[*]")
      .config("spark.memory.fraction", 0.8)
      .config("spark.executor.memory", "2g")
      .getOrCreate()

    val jsonFileDF = spark.read
      .format("json")
      .option("multiline", true)
      .load("/home/ankitagr/Documents/transmissions.json")
    jsonFileDF.printSchema()
    jsonFileDF
      .select("01-mac-b827eb5328c3.parameters.`Device.Bridging.Bridge.1.Alias`")
      .show()
  }
}
