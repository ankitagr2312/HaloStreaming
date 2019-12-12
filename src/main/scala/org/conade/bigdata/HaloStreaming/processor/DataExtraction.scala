package org.conade.bigdata.HaloStreaming.processor

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.conade.bigdata.HaloStreaming.models.configurationModel
import org.conade.bigdata.HaloStreaming.util.SchemaCreator

case class DataExtraction(confModel: configurationModel) {

  def extractData(df: sql.DataFrame, spark: SparkSession): sql.DataFrame = {
    confModel.dataProcessor.`type` match {
      case "json" => {
        //Create schema in advance from sample json data
        val schemaJsonPath = confModel.dataProcessor.properties.get("path")
        val jsonSchema = spark.read
          .format("json")
          .option("multiline", true)
          .load(schemaJsonPath.getOrElse(throw new Exception(
            "Path for sample json is not specified.This is required to create schema for json in Streaming")))
          .schema
        val valueDF = df.selectExpr("CAST(value AS STRING)")
        val convertedDF = valueDF
          .select(from_json(col("value").cast("string"), jsonSchema).as("data"))
          .select("data.*")
        val outputSchemaPath = confModel.dataProcessor.properties
          .get("outputSchemaPath") match {
          case Some(s) => s
          case None =>
            throw new Exception("Path for output schema is not specified.")
        }
        val outputSchema = spark.read.csv(outputSchemaPath).collect()
        val colList = outputSchema.map(row => row(0).toString)

        //TODO: This is not optimized.Refer below link
        // https://lansalo.com/2018/05/13/spark-how-to-add-multiple-columns-in-dataframes-and-how-not-to/
        outputSchema
          .foldLeft(convertedDF)((df, row) => {
            df.withColumn(
              row(0).toString,
              df(row(1).toString).cast(
                SchemaCreator(confModel).getSparkDataType(row(2).toString)))
          })
          .select(colList.head, colList.tail: _*)
// https://stackoverflow.com/questions/44367019/column-name-with-dot-spark for reading columns having dot
      }
    }
  }
}
