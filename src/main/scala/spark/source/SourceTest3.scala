package spark.source


import org.apache.commons.codec.digest.DigestUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.{Row, SparkSession}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.{DefaultFormats, _}
import spark.util.{SexUtil, TimeUtil}


/**
  * @Auther: cxb
  * @Date: 2018/8/29 14:04
  * @Description:
  **/

case class SourceApps3(name: String, v: Option[String]) extends Serializable

case class Location3(`type`: String, value: String, time: String) extends Serializable

case class Cellulars3(cellularType: String, carrier: String, mcc: String, mnc: String, cellId: String, lac: String, signal: String, current: Boolean) extends Serializable

case class Ips3(realIp: Option[String], proxyIp: List[Option[String]]) extends Serializable

case class NetWorks3(activeNet: Option[String], cellular: List[Cellulars3], wifi: List[Wifi3], ip: Ips3) extends Serializable

case class Wifi3(bssid: String, ssid: String, freq: String, signal: String, ip: String, time: String, current: Boolean) extends Serializable

case class Pays3(currencyType: String, paymentType: String, payAmount: String) extends Serializable

case class SourceA3(androidId: Option[String], imei: String, phone: String, mac: String, btMac: String, idfa: String, serialNumber: Option[String], display: String, deviceName: String,
                    memSize: String, storageSize: String, hasSDCard: String, cpuName: String, cpuCount: String, cpuRate: String, battery: String, startTime: String,
                    os: String, osLevel: String, osVersion: String, model: String, sourceAppName: String, sourceAppVersion: String, sourceAppPackageName: String,
                    sourceAppType: String, sourceAppInstallTime: String, activityTimePeriod: String, apps: List[SourceApps3], useApps: List[SourceApps3], sex: String,
                    age: String, marry: String, consumerHobby: String, location: List[Location3], firstUseTime: String, operator: String, createTime: String, netWorks3: NetWorks3, pay: List[Pays3]) extends Serializable

object SourceTest3 {

  private val logger = Logger.getLogger(SourceTest3.getClass)

  implicit val formats = DefaultFormats

  def main(args: Array[String]): Unit = {

    val session = SparkSession
      .builder()
      .master("local[*]")
      //.master("spark://test:7077")
      // .master("spark://dev1.aidata360.com:7077")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    //val path = "E:\\Desktop\\DataSource\\S3\\devices.json"
    val path = "E:\\Desktop\\20180905\\S3\\devices.json"
    val rdd = session.read.textFile(path)

    import session.implicits._


    val rddReturn = rdd.map {
      line =>
        val sourceA3 = parse(line).extract[SourceA3]

        val result = List(1)

        sourceA3 match {
          case sourceA3.apps if !sourceA3.apps.isEmpty => sourceA3.apps
          case _ => ""
        }
        sourceA3 match {
          case sourceA3.location if !sourceA3.location.isEmpty => sourceA3.location
          case _ => ""
        }
        sourceA3 match {
          case sourceA3.useApps if !sourceA3.useApps.isEmpty => sourceA3.useApps
          case _ => ""
        }
        sourceA3 match {
          case sourceA3.pay if !sourceA3.pay.isEmpty => sourceA3.pay
          case _ => ""
        }
        sourceA3 match {
          case sourceA3.netWorks3.ip.proxyIp if !sourceA3.netWorks3.ip.proxyIp.isEmpty => sourceA3.netWorks3.ip.proxyIp
          case _ => ""
        }
        sourceA3 match {
          case sourceA3.netWorks3.wifi if !sourceA3.netWorks3.wifi.isEmpty => sourceA3.netWorks3.wifi
          case _ => ""
        }

        sourceA3 match {
          case sourceA3.netWorks3.cellular if !sourceA3.netWorks3.cellular.isEmpty => sourceA3.netWorks3.cellular
          case _ => ""
        }

        sourceA3 match {
          case sourceA3.netWorks3.cellular if !sourceA3.netWorks3.cellular.isEmpty => sourceA3.netWorks3.cellular
          case _ => ""
        }


        //        /**
        //          * 判断dericeId
        //          */
        //        var dericeId = ""
        //
        //        if (sourceA3.os.contains("android") && sourceA3.imei != null && sourceA3.imei != "") {
        //
        //          dericeId = DigestUtils.md5Hex(sourceA3.imei)
        //
        //
        //        } else if (sourceA3.os.contains("ios") && sourceA3.idfa != null && sourceA3.idfa != "" && !sourceA3.idfa.equals("00000000-0000-0000-0000-000000000000")) {
        //          dericeId = DigestUtils.md5Hex(sourceA3.idfa)
        //
        //        } else if (sourceA3.mac != null && sourceA3.mac != "" && !sourceA3.mac.equals("02:00:00:00:00:00")) {
        //          dericeId = DigestUtils.md5Hex(sourceA3.mac)
        //        } else if (sourceA3.androidId != null) {
        //          dericeId = DigestUtils.md5Hex(sourceA3.androidId)
        //        } else {
        //          dericeId = dericeId
        //        }

        /**
          * 判断model
          */
        var modelNews = ""
        var brand = ""
        if (!sourceA3.model.isEmpty) {
          if (sourceA3.model.contains("|")) {
            val news = sourceA3.model.split("\\|")
            brand = news(0)
            modelNews = news(1)
          } else if (sourceA3.model.contains(" ")) {
            val news = sourceA3.model.split(" ")
            brand = news(0)
            modelNews = news(1)
          } else {
            brand = sourceA3.model
          }
        }

        //判断性别
        val sexNew = SexUtil.tranSex(sourceA3.sex)


        /*
        判断经纬度
         */
        var lng = "" //经度
      var lat = "" //维度
      var typeGps = ""
        var typeIps = "" //ip
      var times = ""


        if (!sourceA3.location.isEmpty) {
          sourceA3.location.map {
            t =>
              if (t.`type`.equals("GPS")) {
                if (t.value.contains("|")) {
                  lng = t.value.split("\\|")(0)
                  lat = t.value.split("\\|")(1)
                  if (lng.contains("NULL")) {
                    lng = lng.replace("NULL", "")
                  }
                  if (lat.contains("NULL")) {
                    lat = lat.replace("NULL", "")
                  }
                  times = t.time
                  typeGps = t.`type`
                }
              } else {
                typeIps = t.value
              }
          }
        }

        /**
          * platform imei不是空就赋值为安卓
          */
        var platform = ""
        if (sourceA3.imei != "") {
          platform = "android"
        }

        val json =
          (
            ("createTime" -> sourceA3.createTime) ~
              ("source" -> "65925549abf32ede06b70f54d4a09c53") ~
              ("platform" -> platform) ~
              ("deviceid" -> "") ~ //生成deviceid
              ("id" ->
                ("aaid" -> "") ~
                  ("androidId" -> sourceA3.androidId.getOrElse("").toLowerCase()) ~
                  ("idfa" -> sourceA3.idfa.toLowerCase()) ~
                  ("btMac" -> sourceA3.btMac) ~
                  ("wifiMac" -> sourceA3.mac.toLowerCase) ~
                  ("imei" -> sourceA3.imei.toLowerCase()) ~
                  ("serialNo" -> sourceA3.serialNumber.getOrElse("").toLowerCase()) ~
                  ("tdid" -> "")
                ) ~
              ("app" ->
                ("appName" -> sourceA3.sourceAppName) ~
                  ("appVersion" -> sourceA3.sourceAppVersion) ~
                  ("pkgName" -> sourceA3.sourceAppPackageName) ~
                  ("installTime" -> sourceA3.sourceAppInstallTime) ~
                  ("firstUseTime" -> sourceA3.firstUseTime) ~
                  ("appList" ->
                    sourceA3.apps.map {
                      apps =>
                        (
                          ("appName" -> "") ~
                            ("packageName" -> apps.name) ~
                            ("appVersion" -> apps.v)
                          )
                    }
                    )
                ) ~
              ("os" ->
                ("osName" -> sourceA3.os) ~
                  ("osVersion" -> sourceA3.osVersion) ~
                  ("osLevel" -> sourceA3.osLevel) ~
                  ("osLanguage" -> "") ~
                  ("osLocale" -> "") ~
                  ("timezone" -> "") ~
                  ("deviceName" -> sourceA3.deviceName)
                ) ~
              ("user" ->
                ("sex" -> sexNew) ~
                  ("age" -> sourceA3.age) ~
                  ("marry" -> sourceA3.marry)
                ) ~
              ("location" ->
                result.map {
                  lo =>
                    ("lat" -> lat) ~ //注意这里的
                      ("lng" -> lng) ~
                      ("alt" -> "") ~
                      ("time" -> times) ~
                      ("hAccuracy" -> "") ~
                      ("vAccuracy" -> "") ~
                      ("bearing" -> "") ~
                      ("speed" -> "") ~
                      ("provider" -> typeGps)
                }
                ) ~
              ("networks" ->
                ("activeNet" -> "") ~
                  ("cellular" ->
                    result.map {
                      cell =>
                        ("cellularType" -> "") ~
                          ("carrier" -> sourceA3.operator) ~
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
                    ("realIp" -> typeIps) ~
                      ("proxyIp" ->
                        result.map {
                          proxy => ""
                        }
                        )
                    )
                ) ~
              ("device" ->
                ("manufacture" -> "") ~
                  ("brand" -> brand) ~
                  ("model" -> modelNews) ~
                  ("deviceType" -> "") ~
                  ("pixel" -> sourceA3.display) ~
                  ("cpu" ->
                    (
                      ("name" -> sourceA3.cpuName) ~
                        ("core" -> sourceA3.cpuCount) ~
                        ("freq" -> sourceA3.cpuRate)
                      )
                    ) ~
                  ("mem" -> sourceA3.memSize) ~
                  ("storage" -> sourceA3.storageSize) ~
                  ("sdCard" -> sourceA3.hasSDCard) ~
                  ("batteryCapacity" -> sourceA3.battery) ~
                  ("bootTime" -> sourceA3.startTime)
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
    rddReturn.take(50).foreach(f => println(f))
    //rddReturn.saveAsTextFile("hdfs://test:9000/checkpoint/s1Out.txt")
    //val outPath =
    // "e://Desktop//Source//S3" + System.currentTimeMillis()
    //val outPath = "E:\\Desktop\\20180905\\S3\\OutS3a"
    // rddReturn.rdd.saveAsTextFile(args(1))
    session.stop()
  }
}

