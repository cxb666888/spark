//package spark.test
//
//
//import java.io.Serializable
//
//import com.alibaba.fastjson.JSON
//import org.apache.log4j.Logger
//import org.apache.spark.sql.{Dataset, SparkSession}
//
//
///**
//  * @Auther: cxb
//  * @Date: 2018/8/27 13:23
//  * @Description: 读取s1文件转成parquet文件存储在hdfs上
//  */
//
//case class User(androidId: String, imei: String, mac: String, btMac: String, idfa: String, serialNumber: String, display: String, deviceName: String, memSize: String, storageSize: String, hasSDCard: String, cpuName: String, cpuCount: String, cpuRate: String, battery: String, startTime: String, os: String, osLevel: String, osVersion: String, model: String, sourceAppName: String, sourceAppVersion: String, sourceAppPackageName: String, sourceAppType: String, sourceAppInstallTime: String, activityTimePeriod: String, apps: String, useApps: String, sex: String, age: String, marry: String, consumerHobby: String, location: String, firstUseTime: String, operator: String, createTime: String) extends Product with Serializable
//
//case class User1(receiveTime: String, source: String, platform: String, id: Id, appList: AppList, os: Os, location: List[Location], networks: Nothing, device: Device)
//
//case class Id(aaid: String, androidId: String, idfa: String, btMac: String, wifiMac: String, imei: String, serialNo: String)
//
//case class Device(manufacture: String, brand: String, model: String, deviceType: String, pixel: String, cpu: Cpu, mem: String, storage: String, sdCard: String, batteryCapacity: String, bootTime: String)
//
//case class Cpu(name: String, core: String, freq: String)
//
//case class Os(osVersion: String, osLevel: String, osLanguage: String, osLocale: String, timezone: String, deviceName: String)
//
//case class Tag(tagName: String, tagInf: String)
//
//case class AppList(run: List[String], install: List[SourceApp])
//
//case class SourceApp(name: String, packageName: String, ver: String)
//
//case class Location(lat: Double, lng: Double, alt: String, time: String, hAccuracy: String, vAccuracy: String, bearing: String, speed: String, provider: String)
//
//
//object Test5 {
//
//  private val logger = Logger.getLogger(Test5.getClass)
//
//  def main(args: Array[String]): Unit = {
//    val session = SparkSession.builder().appName("test3").master("local").getOrCreate()
//    val fiels = session.read.textFile("E:\\Desktop\\数据格式\\s3原始数据格式\\s1.txt")
//    import session.implicits._
//    //session.read.json()
//    val info = fiels.map(x => {
//      val androidId = JSON.parseObject(x).getString("androidId")
//      val imei = JSON.parseObject(x).getString("imei")
//      val mac = JSON.parseObject(x).getString("mac")
//      val btMac = JSON.parseObject(x).getString("btMac")
//      val idfa = JSON.parseObject(x).getString("idfa")
//      val serialNumber = JSON.parseObject(x).getString("serialNumber")
//      val display = JSON.parseObject(x).getString("display")
//      val deviceName = JSON.parseObject(x).getString("deviceName")
//      val memSize = JSON.parseObject(x).getString("memSize")
//      val storageSize = JSON.parseObject(x).getString("storageSize")
//      val hasSDCard = JSON.parseObject(x).getString("hasSDCard")
//      val cpuName = JSON.parseObject(x).getString("cpuName")
//      val cpuCount = JSON.parseObject(x).getString("cpuCount")
//      val cpuRate = JSON.parseObject(x).getString("cpuRate")
//      val battery = JSON.parseObject(x).getString("battery")
//      val startTime = JSON.parseObject(x).getString("startTime")
//      val os = JSON.parseObject(x).getString("os")
//      val osLevel = JSON.parseObject(x).getString("osLevel")
//      val osVersion = JSON.parseObject(x).getString("osVersion")
//      val model = JSON.parseObject(x).getString("model")
//      val sourceAppName = JSON.parseObject(x).getString("sourceAppName")
//      val sourceAppVersion = JSON.parseObject(x).getString("sourceAppVersion")
//      val sourceAppPackageName = JSON.parseObject(x).getString("sourceAppPackageName")
//      val sourceAppType = JSON.parseObject(x).getString("sourceAppType")
//      val sourceAppInstallTime = JSON.parseObject(x).getString("sourceAppInstallTime")
//      val activityTimePeriod = JSON.parseObject(x).getString("activityTimePeriod")
//      val apps = JSON.parseObject(x).getString("apps")
//      val name=apps.split(",")
//     // JSON.parseObject(x)
//
//
//
//      val useApps = JSON.parseObject(x).getString("useApps")
//      val sex = JSON.parseObject(x).getString("sex")
//      val age = JSON.parseObject(x).getString("age")
//      val marry = JSON.parseObject(x).getString("marry")
//      val consumerHobby = JSON.parseObject(x).getString("consumerHobby")
//      val location = JSON.parseObject(x).getString("location")
//      val firstUseTime = JSON.parseObject(x).getString("firstUseTime")
//      val operator = JSON.parseObject(x).getString("operator")
//      val createTime = JSON.parseObject(x).getString("createTime")
//      //User(androidId, imei, mac, btMac, idfa, serialNumber, display, deviceName, memSize, storageSize, hasSDCard, cpuName, cpuCount, cpuRate, battery, startTime, os, osLevel, osVersion, model, sourceAppName, sourceAppVersion, sourceAppPackageName, sourceAppType, sourceAppInstallTime, activityTimePeriod, apps, useApps, sex, age, marry, consumerHobby, location, firstUseTime, operator, createTime)
//      var receiveTime = null
//      var source = null
//      var platform = null
//      //----------------------------
//      var aaid = null
//      var wifiMac = null
//      var serialNo = null
//      //-------------------------
//      var appName = null
//      var appVersion = null
//      var pkgName = null
//      var installTime = null
//      var activeTime = null
//      var activeTimes = null
//      //---------------------------
//      var run:List[String]=null
//      //---------------------------
//      User1(receiveTime, source, platform,
//        Id(aaid, androidId, idfa, btMac, wifiMac, imei, serialNo),
//        //App(appName, appVersion, pkgName, installTime, activeTime, activeTimes),
//        AppList(run,SourceApp(apps)),
//        os: Os, location: List[Location], networks: Nothing, device: Device)
//    }).toDF().as[User]
//    info.rdd.saveAsTextFile("e:\\desktop\\1917")
//    ///info.write.format("parquet").option("delimiter", "\t").save("E:\\Desktop\\数据格式\\201808271612")
//
//  }
//}
