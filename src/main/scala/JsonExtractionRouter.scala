import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer
object JsonExtractionRouter {
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
      .load("src/main/resources/struct_type_record.json")

    val rawData = jsonFileDF.select(col("01-mac-b827eb5328c3.paramsTree.*"))

    val pathList = List(
      "Device.NAT.InterfaceSetting.{i}.Interface",
      "Device.Services.VoiceService.{i}.CallControl.IncomingMap.{i}.ExtensionRef",
      "Device.DHCPv4.Server.Pool.{i}",
      "Device.DHCPv4.Server.Pool.{i}.Client.{i}.Active",
      "Device.DHCPv4.Server.Pool.{i}.Client.{i}.Chaddr",
      "Device.DHCPv4.Server.Pool.{i}.Client.{i}.IPv4Address.{i}",
      "Device.DHCPv4.Server.Pool.{i}.Client.{i}.IPv4Address.{i}.IPAddress",
      "Device.DHCPv4.Server.Pool.{i}.Client.{i}.IPv4Address.{i}.LeaseTimeRemaining",
      "Device.DeviceInfo.ProcessStatus.CPUUsage",
      "Device.DeviceInfo.ProcessStatus.ProcessNumberOfEntries",
      "Device.DeviceInfo.ProductClass",
      "Device.WiFi.AccessPointNumberOfEntries",
      "Device.WiFi.AccessPoint.{i}.AssociatedDevice.{i}.Active",
      "Device.WiFi.AccessPoint.{i}.AssociatedDevice.{i}.AuthenticationState",
      "Device.WiFi.AccessPoint.{i}.AssociatedDevice.{i}.LastDataDownlinkRate",
      "Device.WiFi.AccessPoint.{i}.AssociatedDevice.{i}.LastDataUplinkRate",
      "Device.WiFi.AccessPoint.{i}.AssociatedDevice.{i}.MACAddress",
      "Device.WiFi.AccessPoint.{i}.AssociatedDevice.{i}.Noise",
      "Device.WiFi.AccessPoint.{i}.AssociatedDevice.{i}.OperatingStandard",
      "Device.WiFi.AccessPoint.{i}.AssociatedDevice.{i}.Retransmission",
      "Device.WiFi.AccessPoint.{i}.AssociatedDevice.{i}.SignalStrength",
      "Device.WiFi.AccessPoint.{i}.AssociatedDevice.{i}.X_SMARTNETWORK-TELEKOM-DIENSTE-DE_DiffStats"
      //"Device.WiFi.AccessPoint.{i}.AssociatedDevice.{i}.X_SMARTNETWORK-TELEKOM-DIENSTE-DE_DiffStats.BytesReceived",
      //"Device.WiFi.AccessPoint.{i}.AssociatedDevice.{i}.X_SMARTNETWORK-TELEKOM-DIENSTE-DE_DiffStats.BytesSent",
      //"Device.WiFi.AccessPoint.{i}.AssociatedDevice.{i}.X_SMARTNETWORK-TELEKOM-DIENSTE-DE_DiffStats.IntervalDuration"
    ).sorted.filter(f => !(f.takeRight(3) == "{i}"))

    val hierarchyList =
      scala.collection.mutable.ListBuffer.empty[(String, String, Int)]
    pathList.foreach(child => {
      val lstComp = child.split("\\.")
      hierarchyList.append((lstComp(0) + "." + lstComp(1), child, lstComp.size))
    })
    val groupedList = hierarchyList.groupBy(_._1)

    val processedPathList = scala.collection.mutable.ListBuffer.empty[String]
    val globalHierarchyList =
      scala.collection.mutable.ListBuffer
        .empty[ListBuffer[(String, String, Int)]]
    groupedList.foreach(gp => {
      val sortedList = gp._2.sortBy(_._3)(Ordering[Int].reverse)
      sortedList.foreach(obj => {
        if (!processedPathList.contains(obj._2)) {
          var tempHierarchyList =
            scala.collection.mutable.ListBuffer.empty[(String, String, Int)]
          tempHierarchyList += obj
          val nonIList = sortedList
            .filter(fi => !fi._2.contains("{i}"))
            .filter(fi => !(fi._2 == obj._2))
          tempHierarchyList = tempHierarchyList ++ nonIList
          //Match list is list of element that have same size as of the current Path
          //eg Device.wifi and Device.Right are parallel
          val matchList = sortedList.filter(ele => {
            val currentObjSplitSize = obj._2.split("\\{i\\}").size
            var listObjSplitSize = 0
            if (ele._2.takeRight(3) == "{i}") {
              listObjSplitSize = ele._2.split("\\{i\\}").size + 1
            } else {
              listObjSplitSize = ele._2.split("\\{i\\}").size
            }
            listObjSplitSize == currentObjSplitSize
          })
          if (matchList.size > 1 && obj._2.contains("{i}")) {
            val upperLevel =
              obj._2.split("\\{i\\}").dropRight(1).mkString("{i}")
            matchList
              .filter(
                ob =>
                  ob._2
                    .split("\\{i\\}")
                    .dropRight(1)
                    .mkString("{i}") == upperLevel)
              .filter(ob => ob != obj)
              .foreach(ele => tempHierarchyList += ele)
          }

          /**
            * Now there are two part:
            * 1. Process the Paths that doesnt have {i}
            * 2. Process the Paths that have {i}
            * */
          tempHierarchyList.foreach(hObj => processedPathList += hObj._2)
          globalHierarchyList.append(tempHierarchyList)
        }
      })
      //println()
    })

    val listOfDf = globalHierarchyList
      .map(hList => {
        val df = processData(rawData, hList.map(hObj => hObj._2).toList)
        df.show()
        df
      })
      .toList

    val columnsList = listOfDf.map(df => df.columns.toSet)

    val setOfCols = columnsList.reduceOption((df1, df2) => {
      df1.toSet.union(df2.toSet)
    })

    val resultantDf = listOfDf.reduceOption((df1, df2) => {
      df1
        .select(expr(df1.columns.toSet, setOfCols.get.toSet): _*)
        .union(df2.select(expr(df2.columns.toSet, setOfCols.get.toSet): _*))
    })
    resultantDf.get.show()
  }

  def expr(myCols: Set[String], allCols: Set[String]) = {
    allCols.toList.map(x =>
      x match {
        case x if myCols.contains(x) => col(x)
        case _                       => lit(null).as(x)
    })
  }

  def processData(df: sql.DataFrame, colsLst: List[String]): sql.DataFrame = {
    if (colsLst.filter(fi => fi.contains("{i}")).size > 0) {
      processICols(df, colsLst)
    } else {
      df.select(colsLst.head, colsLst.tail: _*)
    }
  }

  def processICols(df: sql.DataFrame, colsLst: List[String]): sql.DataFrame = {
    val fColPath = colsLst
      .filter(fi => fi.contains("{i}"))
      .sorted(Ordering[String].reverse)(0)
    val numberOfI = fColPath.split("\\{i\\}")
    val iList =
      scala.collection.mutable.ListBuffer
        .empty[(String, scala.collection.mutable.ListBuffer[(String)])]
    for (i <- 1 to numberOfI.size) {
      if (iList.size == 0) {
        val colStr = numberOfI(i - 1) + "*"
        val iListTemp =
          df.select(col(colStr)).schema.fields.map(f => f.name).toList
        iListTemp.foreach(l => {
          iList.append((numberOfI(i - 1) + l, ListBuffer(l)))
        })
      } else {
        val lstRange = iList.size
        for (j <- 0 to lstRange - 1) {
          if (iList(j)._2.size == i - 1) {
            if (!(i == numberOfI.size)) {
              val colStr = iList(j)._1 + numberOfI(i - 1) + "*"
              val iListTemp =
                df.select(col(colStr)).schema.fields.map(f => f.name).toList
              iListTemp.foreach(l => {
                iList.append(
                  (iList(j)._1 + numberOfI(i - 1) + l,
                   iList(j)._2 ++ ListBuffer(l)))
              })
            } else if ((i == numberOfI.size && fColPath.takeRight(3) == "{i}")) {
              val colStr = iList(j)._1 + numberOfI(i - 1) + "*"
              val iListTemp =
                df.select(col(colStr)).schema.fields.map(f => f.name).toList
              iListTemp.foreach(l => {
                iList.append(
                  (iList(j)._1 + numberOfI(i - 1) + l,
                   iList(j)._2 ++ ListBuffer(l)))
              })
            }
          }
        }
      }
    }
    var iListUpdated = scala.collection.mutable.ListBuffer
      .empty[(String, scala.collection.mutable.ListBuffer[(String)])]
    if (fColPath.takeRight(3) == "{i}") {
      iListUpdated = iList.filter(fi => fi._2.size == numberOfI.size)
    } else {
      iListUpdated = iList.filter(fi => fi._2.size == numberOfI.size - 1)
    }
    getDataICols(df, colsLst, iListUpdated)
  }

  def getDataICols(
      df: sql.DataFrame,
      colsLst: List[String],
      hList: ListBuffer[(String, ListBuffer[(String)])]): sql.DataFrame = {
    val nonIList = colsLst.filter(fi => !fi.contains("{i}"))
    val iList =
      colsLst.filter(fi => fi.contains("{i}")).filter(fi => fi.contains(""))
    var iListCols = colsLst
      .filter(fi => fi.contains("{i}"))
      .sorted(Ordering[String].reverse)(0)
      .split("\\{i\\}")
      .map(fi => fi.split("\\.").last)
    if (iListCols.size != 1) {
      iListCols.dropRight(1)
    }
    val listOfLastCols = iList.map(fi => fi.split("\\{i\\}").last)
    val listDataframe = hList.map(fi => {
      var constantColumnList =
        scala.collection.mutable.ListBuffer.empty[org.apache.spark.sql.Column]
      for (i <- 0 to fi._2.size - 1)
        constantColumnList.append((lit(fi._2(i)).as(iListCols(i))))
      val columnList =
        (listOfLastCols.map(lName => fi._1 + lName) ++ nonIList).map(c =>
          col(c))
      val finalList = columnList ++ constantColumnList.toList
      df.select(finalList: _*)
    })
    val finalDf = listDataframe.reduce(_ union _)
    //finalDf.show()
    finalDf
  }
}
