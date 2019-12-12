package org.conade.bigdata.HaloStreaming.util

import org.apache.spark.sql.SparkSession

object ParquetReader {
  def main(args: Array[String]): Unit = {

    val spark  =     SparkSession
      .builder()
      .appName("Reader").master("local[*]")
      .getOrCreate()

    val parquetFileDF = spark.read.parquet("/home/ankitagr/IdeaProjects/HaloStreaming/outputDir")
    parquetFileDF.show()

  }
}
