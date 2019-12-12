package org.conade.bigdata.HaloStreaming.util

import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType}
import org.conade.bigdata.HaloStreaming.models.configurationModel

case class SchemaCreator(confModel: configurationModel) {
  def getSchema(): StructType = {
    val lstSF = confModel.inputSchema.map(tup => structFieldGetter(tup._1, tup._2))
    StructType(lstSF)
  }
  def structFieldGetter(col: String, dataTypeName: Any): StructField = {
    dataTypeName.toString.toLowerCase match {
      case "string" => StructField(col, StringType, true)
      case "int"    => StructField(col, IntegerType, true)
      case "double" => StructField(col, DoubleType, true)
      case "float"  => StructField(col, FloatType, true)
      case d if d.toString.contains("decimal(") => {
        val lstArgs = d.toString.split("\\(")(0).replace("//)", "").split(",")
        StructField(col, DecimalType(lstArgs(0).toInt,lstArgs(1).toInt), true)
      }
      case "long"  => StructField(col, LongType, true)
      case "short" => StructField(col, ShortType, true)
      case "byte" => StructField(col, ByteType, true)
      case "date" =>  StructField(col, DateType, true)
      case "boolean" =>  StructField(col, BooleanType, true)
      case "timestamp" => StructField(col, TimestampType, true)
    }
  }

  def getSparkDataType(dataTypeValue: String): DataType = {
    dataTypeValue match {
      case "string" => StringType
      case "int"    => IntegerType
      case "double" => DoubleType
      case "float"  => FloatType
      case d if d.toString.contains("decimal(") => {
        val lstArgs = d.toString.split("\\(")(0).replace("//)", "").split(",")
        DecimalType(lstArgs(0).toInt,lstArgs(1).toInt)
      }
      case "long"  => LongType
      case "short" => ShortType
      case "byte" => ByteType
      case "date" =>  DateType
      case "boolean" =>  BooleanType
      case "timestamp" => TimestampType
    }
  }
}
