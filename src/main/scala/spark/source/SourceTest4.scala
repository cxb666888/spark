
package com.xiaobin

import org.apache.commons.codec.digest.DigestUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.{DefaultFormats, _}
import spark.util.TimeUtil
//import spark.sourceTest.TimeUtil

import scala.collection.parallel.mutable

case class Cellulars4(cellularType: String, carrier: String, mcc: String, mnc: String, cellId: String, lac: String, signal: String, current: Boolean) extends Serializable

case class Ips4(realIp: Option[String], proxyIp: List[Option[String]]) extends Serializable

case class NetWorks4(activeNet: Option[String], cellular: List[Cellulars4], wifi: List[Wifi4], ip: Ips4) extends Serializable

case class Wifi4(bssid: String, ssid: String, freq: String, signal: String, ip: String, time: String, current: Boolean) extends Serializable

case class Pays4(currencyType: String, paymentType: String, payAmount: String) extends Serializable

case class Locations4(lat: String, lng: String, alt: String, time: String, hAccuracy: String, vAccuracy: String, bearing: String, speed: String, provider: String) extends Serializable

case class SourceApp4(name: String, packageName: List[String], v: Option[String]) extends Serializable


case class SourceA4(falg: String, packageName: List[String], installedApps: String, receiveDate: String, netWorks4: NetWorks4, pay: List[Pays4], location: List[Locations4], apps: List[SourceApp4])


object Source4 {

  implicit val formats = DefaultFormats
  private val logger = Logger.getLogger(Source4.getClass)


  def main(args: Array[String]): Unit = {

    val session = SparkSession
      .builder()
      .master("local[*]")
      //.master("spark://172.31.10.2:7077")
      //.master("spark://dev1.aidata360.com:7077")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    val path = "E:\\Desktop\\DataSource\\S4\\user_n_7.txt"
    //val path = "E:\\Desktop\\数据格式\\s3原始数据格式\\s4.txt"
    val rdd = session.read.textFile(path)
    // val rdd = session.read.textFile(args(0))

    import session.implicits._

    val rddReturn = rdd.map {
      line =>
        val lines = line.split(",")
        val falg = lines(0)
        val packageNameOne = lines(1)
        val receiveDate = lines(2)
        val packageName = packageNameOne.split(";")

        /**
          * 追加com
          */
        /* val soources: Array[Any] = packageName.map(t => {
           if (t.startsWith(".")) {
             var newsPackName = "com" ++: t
             (newsPackName)
           }
         })*/

        //        val sources = packageName.toList.map(t =>
        //          if (!t.isEmpty || !t.equals("") && t.startsWith(".")) {
        //            var newsPackName = "com" ++: t
        //            (newsPackName)
        //          }
        //        )
        val list = for (x <- packageName.toList if (!x.isEmpty || !x.equals(""))) yield x

        val sources = list.map {
          line =>
            val lineRe = if (line.startsWith(".")) {
              "com" + line
            } else {
              line
            }
            lineRe
        }
        //  println(sources.toBuffer)
        var timeNews = ""
        if (receiveDate.length == 19) {
          //将日期转换为时间戳
          timeNews = TimeUtil.tranTimeToLong(receiveDate).toString
        }


        val result = List(1)

        var mac = ""
        var imei = ""
        var dericeId = ""
        var platform = ""

        /**
          * 判断是mac还是imei号   并加密生成deviceid
          */

        if (!falg.isEmpty) {
          if (falg.contains("mac") && !falg.contains("02:00:00:00:00:00")) {
            val falgNews = falg.substring(3, falg.length)
            //println(falgNews + "---111111111111111111111111111")
            if (falgNews.length == 12) {
              mac = falgNews.substring(0, 2) + ":" + falgNews.substring(2, 4) + ":" + falgNews.substring(4, 6) + ":" + falgNews.substring(6, 8) + ":" + falgNews.substring(8, 10) + ":" + falgNews.substring(10, 12)
              //println(mac + "---------------------------------")
            }
            dericeId = DigestUtils.md5Hex(mac.toLowerCase())
          } else {
            imei = falg.toLowerCase
            //platform imei不是空就赋值为安卓
            if (falg != "") {
              platform = "android"
            }
            dericeId = DigestUtils.md5Hex(imei.toLowerCase())
          }
        } else {
          dericeId = dericeId
        }

        val json =
          (
            ("createTime" -> timeNews) ~
              ("source" -> "914b0a7df399ce9051fe773c0ca03d55") ~
              ("platform" -> platform) ~
              ("deviceid" -> dericeId) ~
              ("id" ->
                ("aaid" -> "") ~
                  ("androidId" -> "") ~
                  ("idfa" -> "") ~
                  ("btMac" -> "") ~
                  ("wifiMac" -> mac.toLowerCase()) ~
                  ("imei" -> imei.toLowerCase()) ~
                  ("serialNo" -> "") ~
                  ("tdid" -> "")
                ) ~
              ("app" ->
                ("appName" -> "") ~
                  ("appVersion" -> "") ~
                  ("pkgName" -> "") ~
                  ("installTime" -> "") ~
                  ("firstUseTime" -> "") ~
                  ("appList" ->
                    sources.map {
                      apps =>
                        ("appName" -> "") ~
                          ("packageName" -> apps) ~
                          ("appVersion" -> "")
                    }
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
                      lo =>
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
                        }
                        ) ~
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
                            result.map {
                              proxy => ""
                            }
                            )
                        )
                    ) ~
                  ("device" ->
                    ("manufacture" -> "") ~
                      ("brand" -> "") ~
                      ("model" -> "") ~
                      ("deviceType" -> "") ~
                      ("pixel" -> "") ~
                      ("cpu" ->
                        (
                          ("name" -> "") ~
                            ("core" -> "") ~
                            ("freq" -> "")
                          )
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
            )
        compact(render(json))
    }

    rddReturn.rdd.foreach(f => println(f))
    //    val outPath = "e://Desktop//Source//" + System.currentTimeMillis()
    //    rddReturn.saveAsTextFile(outPath)
    //val outPath = "E:\\Desktop\\20180905\\S4\\OutS4"
    //rddReturn.rdd.saveAsTextFile(args(1))
    session.stop()
  }

}



