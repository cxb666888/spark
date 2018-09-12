package spark.IdfaAndImei


import org.apache.commons.codec.digest.DigestUtils
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, _}
import org.json4s._
import spark.source.SourceTest1

/**
  * @Auther: cxb
  * @Date: 2018/8/30 19:02
  * @Description: success
  */

case class SourceAppsImei(name: String, v: Option[String]) extends Serializable

case class LocationImei(`type`: String, value: String, time: String) extends Serializable

case class CellularsImei(cellularType: String, carrier: String, mcc: String, mnc: String, cellId: String, lac: String, signal: String, current: Boolean) extends Serializable

case class IpsImei(realIp: Option[String], proxyIp: List[Option[String]]) extends Serializable

case class NetWorksImei(activeNet: Option[String], cellular: List[CellularsImei], wifi: List[WifiImei], ip: IpsImei) extends Serializable

case class WifiImei(bssid: String, ssid: String, freq: String, signal: String, ip: String, time: String, current: Boolean) extends Serializable

case class PaysImei(currencyType: String, paymentType: String, payAmount: String) extends Serializable

case class SourceAImei(androidId: String, imei: String, phone: String, mac: String, btMac: String, idfa: String, serialNumber: String, display: String, deviceName: String,
                       memSize: String, storageSize: String, hasSDCard: String, cpuName: String, cpuCount: String, cpuRate: String, battery: String, startTime: String,
                       os: String, osLevel: String, osVersion: String, model: String, sourceAppName: String, sourceAppVersion: String, sourceAppPackageName: String,
                       sourceAppType: String, sourceAppInstallTime: String, activityTimePeriod: String, apps: List[SourceAppsImei], useApps: List[SourceAppsImei], sex: String,
                       age: String, marry: String, consumerHobby: String, location: List[LocationImei], firstUseTime: String, operator: String, createTime: String, netWorksImei: NetWorksImei, pay: List[PaysImei]) extends Serializable


object SourceImei {

  implicit val formats = DefaultFormats
  private val logger = Logger.getLogger(SourceImei.getClass)

  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
     // .master("spark://dev1.aidata360.com:7077")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()
    logger.info("-----------------连接成功------------------")

    val path = "E:\\Desktop\\数据格式\\s3原始数据格式\\imei.txt"
    val rdd = session.read.textFile(path)

    import session.implicits._


    val rddList = rdd.map {
      line =>
        val index = line.indexOf(" ")
        val imei = line.substring(0, index) //得到imei
      val result2 = line.substring(imei.length)

        val imeiList = imei.split(",").toList
        val rddl = imeiList.map {
          line =>
            line + " " + result2
        }
        rddl
    }
    logger.info("-----------------进入下一层------------------")

    val rddFlat = rddList.flatMap(x => x)

    val rddReturn = rddFlat.map {
      line =>
        val index = line.indexOf(" ")
        val imei = line.substring(0, index) //得到imei
      val result2 = line.substring(imei.length)
        val re = result2.replace(" ", ",")
        val newest = re.substring(1, re.length)
        val pkgName: Array[String] = newest.split(",", -1) //得到包名

        val source = pkgName.toList

        val result = List(1)

        var dericeId = ""

        if (imei != null && imei != "") {
          dericeId = DigestUtils.md5Hex(imei.toLowerCase())
        } else {
          dericeId = dericeId
        }
        logger.info("imei"+imei+"-----------------------------------")

        val json =
          (
            ("createTime" -> "") ~
              ("source" -> "imei") ~
              ("platform" -> "") ~
              ("deviceid" -> dericeId) ~
              ("id" ->
                ("aaid" -> "") ~
                  ("androidId" -> "") ~
                  ("idfa" -> "") ~
                  ("btMac" -> "") ~
                  ("wifiMac" -> "") ~
                  ("imei" -> imei.toLowerCase()) ~ //imei
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
                    source.map {
                      apps =>
                        ("appName" -> "") ~
                          ("packageName" -> apps) ~
                          ("appVersion" -> "")
                    }) ~
                  ("os" ->
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
                      location =>
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
                        ("name" -> "") ~
                          ("core" -> "") ~
                          ("freq" -> "")
                        ) ~
                      ("mem" -> "") ~
                      ("storage" -> "") ~
                      ("sdCard" -> "") ~
                      ("batteryCapacity" -> "") ~
                      ("bootTime" -> "")
                    ) ~ ("pay" ->
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

    logger.info("-----------------输出  ------------------")

    //val outPath = "e://Desktop//Source//" + System.currentTimeMillis()
    //rddReturn.rdd.saveTextFile(outPath)
    rddReturn.take(30).foreach(println)
    // val outPath = "e://Desktop//Source//IMEI"
    //rddReturn.rdd.saveAsTextFile(args(1))
    session.close()
  }
}
