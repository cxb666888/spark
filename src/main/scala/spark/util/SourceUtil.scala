package spark.util

import org.apache.commons.codec.digest.DigestUtils

/**
  * @Auther: cxb
  * @Date: 2018/9/1 13:52
  * @Description:
  */
object SourceUtil {

  //SourceTest1
  case class SourceApp1(name: String, packageName: String, ver: String) extends Serializable


  case class Location1(`type`: Option[String], value: Option[String], time: Option[String]) extends Serializable


  case class SourceA1(androidId: Option[String], imei: Option[String], mac: Option[String], btMac: Option[String], idfa: Option[String], serialNumber: Option[String], display: Option[String], deviceName: Option[String],
                      memSize: Option[String], storageSize: Option[String], hasSDCard: Option[String], cpuName: Option[String], cpuCount: Option[String], cpuRate: Option[String], battery: Option[String], startTime: Option[String],
                      os: Option[String], osLevel: Option[String], osVersion: Option[String], model: Option[String], sourceAppName: Option[String], sourceAppVersion: Option[String], sourceAppPackageName: Option[String],
                      sourceAppType: Option[String], sourceAppInstallTime: Option[String], activityTimePeriod: Option[String], apps: List[SourceApp1], useApps: List[SourceApp1], sex: Option[String],
                      age: Option[String], marry: Option[String], consumerHobby: Option[String], location: List[Location1], firstUseTime: Option[String], operator: Option[String], createTime: Option[String], pay: List[Pays], cellulars: List[Cellulars], wifi: List[Wifi], ip: Ips) extends Serializable


  case class Pays(currencyType: Option[String], paymentType: Option[String], payAmount: Option[String]) extends Serializable

  case class Cellulars(cellularType: Option[String], carrier: Option[String], mcc: Option[String], mnc: Option[String], cellId: Option[String], lac: Option[String], signal: Option[String], current: Boolean) extends Serializable

  case class Wifi(bssid: Option[String], ssid: Option[String], freq: Option[String], signal: Option[String], ip: Option[String], time: Option[String], current: Boolean) extends Serializable

  case class Ips(realIp: Option[String], proxyIp: List[Option[String]]) extends Serializable

  //SourceTest2
  case class SourceTest2(sourceAppPackageName: String, apps: Map[String, String], time: String, model: String, imei: String, os: String, sourceAppVersion: String, serialNumber: String, pay: List[Pays], cellulars: List[Cellulars], wifi: List[Wifi], ip: Ips, location: List[Locations], netWorks: NetWorks)

  case class Locations(lat: String, lng: String, alt: String, time: String, hAccuracy: String, vAccuracy: String, bearing: String, speed: String, provider: String) extends Serializable

  case class NetWorks(activeNet: Option[String], cellular: List[Cellulars], wifi: List[Wifi], ip: Ips) extends Serializable


}

