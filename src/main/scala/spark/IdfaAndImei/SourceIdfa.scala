package spark.IdfaAndImei

import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, render}

/**
  * @Auther: cxb
  * @Date: 2018/8/30 18:34
  * @Description: success
  */


object SourceIdfa {
  def main(args: Array[String]): Unit = {

    val session = SparkSession
      .builder()
      .master("local[*]")
      //.master("spark://test:7077")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    val path = "E:\\Desktop\\数据格式\\s3原始数据格式\\idfa.txt"
    val rdd = session.read.textFile(path)

    import session.implicits._

    val rddReturn = rdd.map {
      line =>
        var dericeId = ""

        if (line != null && line != "") {
          dericeId = DigestUtils.md5Hex(line)
        }

        val source = List(line)
        val json =
          (
            ("createTime" -> "") ~
              ("source" -> "idfa") ~
              ("platform" -> "") ~
              ("deviceid" -> dericeId) ~
              ("id" ->
                ("aaid" -> "") ~
                  ("androidId" -> "") ~
                  ("idfa" -> line) ~
                  ("btMac" -> "") ~
                  ("wifiMac" -> "") ~
                  ("imei" -> "") ~
                  ("serialNo" -> "") ~
                  ("tdid" -> "")
                ) ~
              ("app" ->
                ("appName" -> "") ~
                  ("appVersion" -> "") ~
                  ("pkgName" -> "") ~
                  ("installTime" -> "") ~
                  ("firstUseTime" -> ""
                    ) ~
                  ("appList" ->
                    source.map {
                      apps =>
                        (
                          ("appName" -> "") ~
                            ("packageName" -> "") ~
                            ("appVersion" -> "")
                          )
                    }
                    ) ~
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
                    source.map {
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
                        source.map {
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
                        source.map {
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
                            source.map {
                              proxy =>
                                ("")
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
                    ) ~
                  ("pay" ->
                    source.map {
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

    // rddReturn.take(10).foreach(f => println(f))
    //    val outPath = "e://Desktop//Source//" + System.currentTimeMillis()
    //    rddReturn.toDF().write.parquet(outPath)
    val outPath = "e://Desktop//Source//IDFA"
    rddReturn.rdd.saveAsTextFile(outPath)
    session.close()
  }
}
