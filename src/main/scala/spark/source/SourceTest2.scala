package spark.source

import org.apache.commons.codec.digest.DigestUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.{DefaultFormats, _}
import spark.util.SourceUtil.SourceTest2

/**
  * @Auther: cxb
  * @Date: 2018/8/31 17:29
  * @Description: success
  */


object SourceTest2 {
  implicit val formats = DefaultFormats
  private val logger = Logger.getLogger(SourceTest2.getClass)

  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      //.master("spark://test:7077")
      //.master("spark://dev1.aidata360.com:7077")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    val path = "E:\\Desktop\\DataSource\\S2\\20180725_data_47.txt.gz"
    val rdd = session.read.textFile(path)

    import session.implicits._

    val rddReturn = rdd.map {
      line =>
        val source2 = parse(line).extract[SourceTest2]
        var dericeId = ""

        val result = List(1)
        /*
                /**
                  * 加密生成dericeid  只有imei号
                  */

                if (source2.os.contains("android") && source2.imei != null && source2.imei != "") {
                  dericeId = DigestUtils.md5Hex(source2.imei)
                } else if (source2.os != null && source2.os.equals("android")) {
                  dericeId = DigestUtils.md5Hex(source2.os)
                } else {
                  dericeId = dericeId
                }
        */

        /**
          * 判断model
          */
        var modelNews = ""
        var brand = ""
        if (!source2.model.isEmpty) {
          if (source2.model.contains("|")) {
            val news = source2.model.split("\\|")
            brand = news(0)
            modelNews = news(1)
          } else if (source2.model.contains(" ")) {
            val news = source2.model.split(" ")
            brand = news(0)
            modelNews = news(1)
          } else {
            brand = source2.model
          }
        }


        source2.ip.proxyIp match {
          case source2.ip.proxyIp if !source2.ip.proxyIp.isEmpty => source2.ip.proxyIp
          case _ => ""
        }

        source2.wifi match {
          case source2.wifi if !source2.wifi.isEmpty => source2.wifi
          case _ => ""
        }

        source2.cellulars match {
          case source2.cellulars if !source2.cellulars.isEmpty => source2.cellulars
          case _ => ""
        }
        source2.pay match {
          case source2.pay if !source2.pay.isEmpty => source2.pay
          case _ => ""
        }

        val json =
          (
            ("createTime" -> source2.time) ~
              ("source" -> "6e293b214a9af7525389b393457365f4") ~
              ("platform" -> source2.os) ~
              ("deviceid" -> dericeId) ~
              ("id" ->
                ("aaid" -> "") ~
                  ("androidId" -> "") ~
                  ("idfa" -> "") ~
                  ("btMac" -> "") ~
                  ("wifiMac" -> "") ~
                  ("imei" -> source2.imei.toLowerCase()) ~
                  ("serialNo" -> source2.serialNumber.toLowerCase()) ~
                  ("tdid" -> "")
                ) ~
              ("app" ->
                ("appName" -> source2.os) ~
                  ("appVersion" -> source2.sourceAppVersion) ~
                  ("pkgName" -> source2.sourceAppPackageName) ~
                  ("installTime" -> "") ~
                  ("firstUseTime" -> "") ~
                  ("appList" ->
                    source2.apps.map {
                      app =>
                        ("appName" -> "") ~
                          ("pkgName" -> app._1) ~
                          ("appVersion" -> app._2)
                    })
                ) ~
              ("os" ->
                ("osName" -> "") ~
                  ("osVersion" -> "") ~
                  ("osLevel" -> "") ~
                  ("osLanguage" -> "") ~
                  ("osLocale" -> "") ~
                  ("timezone" -> "") ~
                  ("deviceName" -> "")
                ) ~
              ("user" ->
                ("sex" -> "") ~
                  ("age" -> "") ~
                  ("marry" -> "")
                ) ~
              ("location" ->
                result.map {
                  loca =>
                    ("lat" -> "") ~
                      ("lng" -> "") ~
                      ("alt" -> "") ~
                      ("time" -> "") ~
                      ("hAccuracy" -> "") ~
                      ("vAccuracy" -> "") ~
                      ("bearing" -> "") ~
                      ("speed" -> "") ~
                      ("provider" -> "")
                }
                ) ~
              ("networks" ->
                ("activeNet" -> "") ~
                  ("cellular" ->
                    result.map {
                      cell =>
                        ("cellularType" -> "") ~
                          ("carrier" -> "") ~
                          ("mcc" -> "") ~
                          ("mnc" -> "") ~
                          ("cellId" -> "") ~
                          ("lac" -> "") ~
                          ("signal" -> "") ~
                          ("current" -> "")
                    }) ~
                  ("wifi" ->
                    result.map {
                      wif =>
                        ("bssid" -> "") ~
                          ("ssid" -> "") ~
                          ("freq" -> "") ~
                          ("signal" -> "") ~
                          ("ip" -> "") ~
                          ("time" -> "") ~
                          ("current" -> "")
                    }
                    ) ~
                  ("ip" ->
                    ("realIp" -> "") ~
                      ("proxyIp" ->
                        result.map { proxy => "" }
                        )
                    )
                ) ~
              ("device" ->
                ("manufacture" -> "") ~
                  ("brand" -> brand) ~
                  ("model" -> modelNews) ~
                  ("deviceType" -> "") ~
                  ("pixel" -> "") ~
                  ("cpu" ->
                    ("name" -> "") ~
                      ("core" -> "") ~
                      ("freq" -> "")
                    ) ~
                  ("mem" -> "") ~
                  ("storage" -> "") ~
                  ("sdCard" -> "") ~
                  ("batteryCapacity" -> "") ~
                  ("bootTime" -> "")
                ) ~
              ("pay" ->
                result.map {
                  pays =>
                    ("currencyType" -> "") ~
                      ("paymentType" -> "") ~
                      ("payAmount" -> "")
                }
                )
            )
        compact(render(json))
    }
    rddReturn.take(30).foreach(f => println(f))
    //    val outPath = "e://Desktop//Source//" + System.currentTimeMillis()
    //    rddReturn.rdd.saveAsTextFile(outPath)
    //val outPath = "E:\\Desktop\\20180905\\S2\\OutS2"
    //rddReturn.rdd.saveAsTextFile(args(1))
    session.stop()
  }
}
