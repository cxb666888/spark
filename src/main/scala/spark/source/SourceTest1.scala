package spark.source


import spark.util.SourceUtil.{Location1, SourceA1, SourceApp1}
import org.apache.commons.codec.digest.DigestUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.{Row, SparkSession}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.{DefaultFormats, _}

/**
  * @Auther: cxb
  * @Date: 2018/8/28 09:28
  * @Description: success
  */
object SourceTest1 {

  implicit val formats = DefaultFormats
  private val logger = Logger.getLogger(SourceTest1.getClass)

  def main(args: Array[String]): Unit = {

    val session = SparkSession
      .builder()
      .master("local[*]")
      // .master("spark://172.31.10.2:7077")
      // .master("spark://dev1.aidata360.com:7077")
      //.master("spark://52.81.28.179:7077")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    logger.info("---------------------------连接成功---------------------------------")

    val path = "E:\\Desktop\\DataSource\\S1\\data-2018-07-12.22.log.gz"
    // val path = "hdfs://dev1.aidata360.com:9000/data/raw/yybucket-test/2018/09/05/data-2018-09-05.1.log.gz"

    val rdd = session.read.textFile(path)
    logger.info("---------------------------开始读取文件---------------------------------")

    import session.implicits._

    val rddReturn = rdd.map {
      line =>
        val p: SourceA1 = parse(line).extract[SourceA1]

        val result = List(1)

        //        var listApp = Nil
        var listApp = if (p.apps.isEmpty) {
          List[SourceApp1](SourceApp1("", "", ""))
        } else {
          val x = for (x <- p.apps) yield x
          x
        }


        p.location match {
          case p.location if !p.location.isEmpty => p.location
          case _ => ""
        }

        p.useApps match {
          case p.useApps if !p.useApps.isEmpty => p.useApps
          case _ => ""
        }

        p.ip.proxyIp match {
          case p.ip.proxyIp if !p.ip.proxyIp.isEmpty => p.ip.proxyIp
          case _ => ""
        }

        p.wifi match {
          case p.wifi if !p.wifi.isEmpty => p.wifi
          case _ => ""
        }

        p.cellulars match {
          case p.cellulars if !p.cellulars.isEmpty => p.cellulars
          case _ => ""
        }

        p.pay match {
          case p.pay if !p.pay.isEmpty => p.pay
          case _ => ""
        }


        /*
        * 拆分model
        * */
        var modelNews = ""
        var brand = ""

        if (!p.model.isEmpty) {
          if (p.model.getOrElse("").contains("|")) {
            val news = p.model.getOrElse("").split("\\|")
            brand = news(0)
            modelNews = news(1)
          } else if (p.model.contains(" ")) {
            val news = p.model.getOrElse("").split(" ")
            brand = news(0)
            modelNews = news(1)
          } else {
            brand = p.model.getOrElse("")
          }
        }


        var lng = "" //经度
      var lat = "" //维度
      var typeGps = ""
        var typeIps = "" //ip
      var times = ""
        //

        if (!p.location.isEmpty) {
          p.location.map {
            t =>
              if (t.`type`.getOrElse("").equals("GPS")) {
                if (t.value.getOrElse("").contains("|")) {
                  lng = t.value.getOrElse("").split("\\|")(0)
                  lat = t.value.getOrElse("").split("\\|")(1)
                  if (lng.contains("NULL")) {
                    lng = lng.replace("NULL", "")
                  }
                  if (lat.contains("NULL")) {
                    lat = lat.replace("NULL", "")
                  }
                  times = t.time.getOrElse("")
                  typeGps = t.`type`.getOrElse("")
                }
              } else {
                typeIps = t.value.getOrElse("")
              }
          }
        }


        /*
        * 判断性别
        * */
        var sexNews = ""
        if (p.sex.getOrElse("") != null && p.sex.getOrElse("") != "") {
          if (p.sex.getOrElse("").equals("Female") || p.sex.getOrElse("").equals("0")) { //女性
            sexNews = "0"
          } else {
            sexNews = "1"
          }
        }

        //        if (null == mac || mac.equals("02:00:00:00:00:00")) sourceUserText.setMac("")
        //        if (null == idfa || idfa.equals("00000000-0000-0000-0000-000000000000")) {
        //        }

        /*
          * 加密生成dericeid
          * IDFA大写，serialNo大写
          * imei、wifiMac、Androidid全部小写
          */
        var dericeId = ""

        if (p.os.contains("ios") && p.idfa != null && p.idfa != "" && !p.idfa.equals("00000000-0000-0000-0000-000000000000") && !p.idfa.equals("0")) {
          dericeId = DigestUtils.md5Hex(p.idfa.getOrElse("").toLowerCase())
        } else if(p.os.equals("")&& p.idfa != null && p.idfa != "" && !p.idfa.equals("00000000-0000-0000-0000-000000000000") && !p.idfa.equals("0")){
          dericeId = DigestUtils.md5Hex(p.idfa.getOrElse("").toLowerCase())
        } else if (p.androidId != null && p.os.equals("android")) {
        dericeId = DigestUtils.md5Hex(p.androidId.getOrElse("").toLowerCase())
      } else {
        dericeId = dericeId
      }


        if (p.os.contains("android") && p.imei != null && p.imei != "") {
          dericeId = DigestUtils.md5Hex(p.imei.getOrElse("").toLowerCase())
        } else if(p.os.equals("") && p.imei != null && p.imei != "") {
          dericeId = DigestUtils.md5Hex(p.imei.getOrElse("").toLowerCase())
        }else if (p.mac != null && p.mac != "" && !p.mac.equals("02:00:00:00:00:00")) {
          dericeId = DigestUtils.md5Hex(p.mac.getOrElse("").toLowerCase())
        } else if (p.androidId != null && p.os.equals("android")) {
          dericeId = DigestUtils.md5Hex(p.androidId.getOrElse("").toLowerCase())
        } else {
          dericeId = dericeId
        }

        if (p.mac != null && p.mac != "" && !p.mac.equals("02:00:00:00:00:00")) {
          dericeId = DigestUtils.md5Hex(p.mac.getOrElse("").toLowerCase())
        } else if (p.androidId != null && p.os.equals("android")) {
          dericeId = DigestUtils.md5Hex(p.androidId.getOrElse("").toLowerCase())
        } else {
          dericeId = dericeId
        }

        if (p.androidId != null && p.os.equals("android")) {
          dericeId = DigestUtils.md5Hex(p.androidId.getOrElse("").toLowerCase())
        } else {
          dericeId = dericeId
        }

        logger.info("---------------------------开始生成json---------------------------------")

        val json =
          (
            ("createTime" -> p.createTime.getOrElse("")) ~
              ("source" -> "859b752014d799232ceff969a908c43c") ~
              ("platform" -> p.os.getOrElse("")) ~
              ("deviceid" -> dericeId) ~
              ("id" ->
                ("aaid" -> "") ~
                  ("androidId" -> p.androidId.getOrElse("")) ~
                  ("idfa" -> p.idfa.getOrElse("").toUpperCase()) ~
                  ("btMac" -> p.btMac.getOrElse("").toLowerCase) ~
                  ("wifiMac" -> p.mac.getOrElse("").toLowerCase()) ~
                  ("imei" -> p.imei.getOrElse("").toLowerCase) ~
                  ("serialNo" -> p.serialNumber.getOrElse("").toUpperCase()) ~
                  ("tdid" -> "")
                ) ~
              ("app" ->
                ("appName" -> p.sourceAppName.getOrElse("")) ~
                  ("appVersion" -> p.sourceAppVersion.getOrElse("")) ~
                  ("pkgName" -> p.sourceAppPackageName.getOrElse("")) ~
                  ("installTime" -> p.sourceAppInstallTime.getOrElse("")) ~
                  ("firstUseTime" -> p.firstUseTime.getOrElse("")) ~
                  ("appList" ->
                    listApp.map {
                      apps =>
                        (
                          ("appName" -> apps.name) ~
                            ("pkgName" -> apps.packageName) ~
                            ("appVersion" -> apps.ver)
                          )
                    }
                    )
                ) ~

              ("os" ->
                ("osName" -> p.os.getOrElse("")) ~
                  ("osVersion" -> p.osVersion.getOrElse("")) ~
                  ("osLevel" -> p.osLevel.getOrElse("")) ~
                  ("osLanguage" -> "") ~
                  ("osLocale" -> "") ~
                  ("timezone" -> "") ~
                  ("deviceName" -> p.deviceName.getOrElse(""))
                ) ~
              ("user" ->
                ("sex" -> sexNews) ~ //p.sex
                  ("age" -> p.age.getOrElse("")) ~
                  ("marry" -> p.marry.getOrElse(""))
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
                          ("carrier" -> p.operator.getOrElse("")) ~
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
                  ("model" -> modelNews) ~ //?
                  ("deviceType" -> "") ~
                  ("pixel" -> p.display.getOrElse("")) ~
                  ("cpu" ->
                    (
                      ("name" -> p.cpuName.getOrElse("")) ~
                        ("core" -> p.cpuCount.getOrElse("")) ~
                        ("freq" -> p.cpuRate.getOrElse(""))
                      )
                    ) ~
                  ("mem" -> p.memSize.getOrElse("")) ~
                  ("storage" -> p.storageSize.getOrElse("")) ~
                  ("sdCard" -> p.hasSDCard.getOrElse("")) ~
                  ("batteryCapacity" -> p.battery.getOrElse("")) ~
                  ("bootTime" -> p.startTime.getOrElse(""))
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


    logger.info("---------------------------落盘到hdfs---------------------------------")
    //rddReturn.take(30).foreach(f => println(f))
    // val outPath = "hdfs://test:9000/checkpoint/" + System.currentTimeMillis()
    val outPath = "E:\\Desktop\\DataSource\\S1" + System.currentTimeMillis()
    //val outPath = "E://Desktop/1402"
    // rddReturn.rdd.saveAsTextFile(args(1))
    rddReturn.rdd.saveAsTextFile(outPath)
    session.stop()
  }
}

