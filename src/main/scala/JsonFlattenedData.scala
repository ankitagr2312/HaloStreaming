import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, lit}
import play.api.libs.json._

import scala.collection.mutable.ListBuffer
import scala.io.Source

object JsonFlattenedData {
  def main(args: Array[String]): Unit = {

    val jsonPath = "src/main/resources/struct_type_record.json"
    val jsonFile = Source.fromFile(jsonPath).getLines.mkString
    val parseJson = Json.parse(jsonFile)
    val flattenJson = JsFlattener.flatten(parseJson)
    val convertedMap = flattenJson.value
      .map(fi => {
        val ele2: String = fi._2 match {
          case obj: JsString     => obj.value
          case obj: JsNumber     => obj.value.toString()
          case obj: JsNull.type  => null
          case obj: JsTrue.type  => "true"
          case obj: JsArray      => obj.value.toString()
          case obj: JsFalse.type => "false"
        }
        val key = fi._1.replace("01-mac-b827eb5328c3.paramsTree.", "")
        (key, ele2)
      })
      .asInstanceOf[Map[String, String]]

    val pathList = List(
      ("Device.NAT.InterfaceSetting.{i}.Interface", "Interface"),
      ("Device.Services.VoiceService.{i}.CallControl.IncomingMap.{i}.ExtensionRef",
       "ExtensionRef"),
      ("Device.DHCPv4.Server.Pool.{i}.Client.{i}.Active", "Active"),
      ("Device.DHCPv4.Server.Pool.{i}.Client.{i}.Chaddr", "Chaddr"),
      ("Device.DHCPv4.Server.Pool.{i}.Client.{i}.IPv4Address.{i}", "NoneVal"),
      ("Device.DHCPv4.Server.Pool.{i}.Client.{i}.IPv4Address.{i}.IPAddress",
       "IPAddress"),
      ("Device.DHCPv4.Server.Pool.{i}.Client.{i}.IPv4Address.{i}.LeaseTimeRemaining",
       "LeaseTimeRemaining"),
      ("Device.DeviceInfo.ProcessStatus.CPUUsage", "CPUUsage"),
      ("Device.DeviceInfo.ProcessStatus.ProcessNumberOfEntries",
       "ProcessNumberOfEntries"),
      ("Device.DeviceInfo.ProductClass", "ProductClass"),
      ("Device.WiFi.AccessPointNumberOfEntries", "AccessPointNumberOfEntries"),
      ("Device.WiFi.AccessPoint.{i}.AssociatedDevice.{i}.Active",
       "Wifi_Active"),
      ("Device.WiFi.AccessPoint.{i}.AssociatedDevice.{i}.AuthenticationState",
       "AuthenticationState"),
      ("Device.WiFi.AccessPoint.{i}.AssociatedDevice.{i}.LastDataDownlinkRate",
       "LastDataDownlinkRate"),
      ("Device.WiFi.AccessPoint.{i}.AssociatedDevice.{i}.LastDataUplinkRate",
       "LastDataUplinkRate"),
      ("Device.WiFi.AccessPoint.{i}.AssociatedDevice.{i}.MACAddress",
       "MACAddress"),
      ("Device.WiFi.AccessPoint.{i}.AssociatedDevice.{i}.Noise", "Noise"),
      ("Device.WiFi.AccessPoint.{i}.AssociatedDevice.{i}.OperatingStandard",
       "OperatingStandard"),
      ("Device.WiFi.AccessPoint.{i}.AssociatedDevice.{i}.Retransmission",
       "Retransmission"),
      ("Device.WiFi.AccessPoint.{i}.AssociatedDevice.{i}.SignalStrength",
       "SignalStrength"),
      ("Device.WiFi.AccessPoint.{i}.AssociatedDevice.{i}.X_SMARTNETWORK-TELEKOM-DIENSTE-DE_DiffStats",
       "X_SMARTNETWORK-TELEKOM-DIENSTE-DE_DiffStats")
    ).sorted.filter(f => !(f._1.takeRight(3) == "{i}"))

    val hierarchyList =
      scala.collection.mutable.ListBuffer.empty[(String, String, String, Int)]
    pathList.foreach(child => {
      val lstComp = child._1.split("\\.")
      hierarchyList.append(
        (lstComp(0) + "." + lstComp(1), child._1, child._2, lstComp.size))
    })
    val groupedList = hierarchyList.groupBy(_._1)

    val processedPathList = scala.collection.mutable.ListBuffer.empty[String]
    val globalHierarchyList =
      scala.collection.mutable.ListBuffer
        .empty[ListBuffer[(String, String, String, Int)]]

    groupedList.foreach(gp => {
      val sortedList = gp._2.sortBy(fi =>
        "\\{i\\}".r.findAllMatchIn(fi._2).length)(Ordering[Int].reverse)
      sortedList.foreach(obj => {
        if (!processedPathList.contains(obj._2)) {
          var tempHierarchyList =
            scala.collection.mutable.ListBuffer
              .empty[(String, String, String, Int)]
          tempHierarchyList += obj
          val nonIList = sortedList
            .filter(fi => !fi._2.contains("{i}"))
            .filter(fi => !(fi._2 == obj._2))
          tempHierarchyList = tempHierarchyList ++ nonIList
          val numOfI = obj._2.split("\\{i\\}")
          for (i <- 1 to numOfI.length - 1) {
            val pathToFind =
              numOfI.dropRight(i).mkString("{i}").concat("{i}")
            val colFilterList = sortedList.filter(fi => {
              val fiSize = fi._2.split("\\{i\\}").size
              val pathToFindSize = pathToFind.split("\\{i\\}").size + 1
              (fi._2.contains(pathToFind)) && (fiSize == pathToFindSize)
            })
            colFilterList
              .filter(fi => fi._2 != obj._2)
              .foreach(ele => tempHierarchyList.append(ele))
          }
          tempHierarchyList.foreach(hObj => processedPathList += hObj._2)
          globalHierarchyList.append(tempHierarchyList)
        }
      })
    })

/*    globalHierarchyList.foreach(fi => {
      fi.foreach(println)
      println()
    })*/

    val completeColList = pathList.map(fi => fi._2)
    globalHierarchyList.foreach(hList => {
      val someData =
        getRows(convertedMap,
                hList.map(fi => (fi._2, fi._3)).toList,
                completeColList)

      someData.foreach(println)
      println()
      println()
    })
  }

  def getRows(data: Map[String, String],
              colsLst: List[(String, String)],
              completeColLst: List[String]): List[List[(String, String)]] = {
    if (colsLst.filter(fi => fi._1.contains("{i}")).size > 0) {
      processICols(data, colsLst, completeColLst)
    } else {
      val mapOfNonICols =
        colsLst.map(keyCol => (keyCol._2, data.get(keyCol._1).get))
      val remainingCols = completeColLst.filter(fi =>
        !(mapOfNonICols.filter(fa => fa._2 == fi).size > 0))
      val mapOfRemaingCols = remainingCols.map(fi => (fi, null))
      val finalData = mapOfNonICols ::: mapOfRemaingCols
      List(finalData)
    }
  }

  def processICols(
      data: Map[String, String],
      colsLst: List[(String, String)],
      completeColLst: List[String]): List[List[(String, String)]] = {
    val fColPath = colsLst
      .filter(fi => fi._1.contains("{i}"))
      .sortBy(str => "\\{i\\}".r.findAllMatchIn(str._1).length)(
        Ordering[Int].reverse)(0)
    val allRows = colsLst
      .map(fi => {
        getRowsHavingSamePattern(data, createRegex(fi._1))
      })
      .flatMap(ele => ele)
    rowGenerator(allRows.toMap, colsLst, completeColLst)
  }

  def rowGenerator(
      data: Map[String, String],
      colsLst: List[(String, String)],
      completeColLst: List[String]): List[List[(String, String)]] = {

    val processedColsList =
      scala.collection.mutable.ListBuffer.empty[String]
    val nonIColLst = colsLst
      .filter(fi => !fi._1.contains("{i}"))
    val withIColLst = colsLst
      .filter(fi => fi._1.contains("{i}"))
    val dataCountColsList =
      scala.collection.mutable.ListBuffer.empty[((String, String), Int)]
    withIColLst.foreach(fi => {
      val someLst = getRowsHavingSamePattern(data, createRegex(fi._1))
      dataCountColsList.append(((fi._1, fi._2), someLst.size))
    })

    val dataCollection =
      scala.collection.mutable.ListBuffer.empty[List[(String, String)]]
    //val nonIList = colsLst.filter(fi => !fi._1.contains("{i}"))
    val fColPath = dataCountColsList
      .filter(fi => fi._1._1.contains("{i}"))
      .filter(fi => fi._2 != 0)
      .sortBy(str => "\\{i\\}".r.findAllMatchIn(str._1._1).length)(
        Ordering[Int].reverse)(0)
    val fColLst = withIColLst.filter(fi =>
      !(dataCountColsList.filter(fa => fa._1._1 == fi._1)(0)._2 == 0))
    val fNulColLst = withIColLst.filter(fi =>
      dataCountColsList.filter(fa => fa._1._1 == fi._1)(0)._2 == 0)

    val fColData =
      getRowsHavingSamePattern(data, createRegex(fColPath._1._1)).toList
        .sortBy(_._1)

    val numOfI = fColPath._1._1.split("\\{i\\}")
    fColData.foreach(fi => {
      if (!processedColsList.contains(fi._1)) {
        val iValLst = (""".(\d{1,}).""".r findAllIn fi._1).toList
          .filter(fi => {
            fi.endsWith(".") && fi.startsWith(".")
          })
          .map(fi => fi.replace(".", ""))

        val mapOfICol = withIColLst.map(colPathObj => {
          val columnPath = colPathObj._1
          val lstOfPartCol = columnPath.split("\\{i\\}")
          var strToMake = ""
          for (i <- 0 to lstOfPartCol.size - 1) {
            if (i != lstOfPartCol.size - 1)
              strToMake = strToMake + lstOfPartCol(i) + iValLst(i)
            else
              strToMake = strToMake + lstOfPartCol(i)
          }
          val valueOfKey = data.get(strToMake).getOrElse(null)
          processedColsList.append(strToMake)
          (colPathObj._2, valueOfKey)
        })
        val mapOfNonICols = nonIColLst.map(colPathObj => {
          (colPathObj._2, data.get(colPathObj._1).getOrElse(null))
        })
        val mapZeroCountCols = fNulColLst.map(colPathObj => {
          (colPathObj._2, null)
        })
        val iBeforeCols =
          scala.collection.mutable.ListBuffer.empty[(String, String)]
        for (i <- 0 to numOfI.size - 2) {
          val keyCol = numOfI(i).split("\\.").takeRight(1).mkString
          iBeforeCols.append((keyCol, iValLst(i)))
        }
        val resultantMap = mapOfICol ++ mapOfNonICols ++ mapZeroCountCols ++ iBeforeCols.toMap
        val remainingCols = completeColLst.filter(fi =>
          !(resultantMap.filter(fa => fa._1 == fi).size > 0))
        val mapOfRemainingCols = remainingCols.map(fi => (fi, null))
        val finalMapOfAllColumns = resultantMap ++ mapOfRemainingCols
        dataCollection.append(finalMapOfAllColumns)
      }
    })

    dataCollection.toList
  }

  def getRowsHavingSamePattern(data: Map[String, String],
                               patternStr: String): Map[String, String] = {
    data
      .filter(fi => {
        val patternRegex = createRegex(patternStr)
        patternRegex.r.findAllIn(fi._1).size > 0
      })
      .toList
      .distinct
      .toMap

    data.filter(fi => {
      val patternRegex = createRegex(patternStr)
      fi._1.matches(patternRegex)
    })
  }

  def createRegex(str: String): String = {
    val someStr = str.replace(".", "(.)")
    val replacementStr = "(\\\\" + "d{1,})"
    someStr.replaceAll("\\{i}", replacementStr)
  }
}
