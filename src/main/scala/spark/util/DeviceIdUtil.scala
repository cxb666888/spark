package spark.util

import org.apache.commons.codec.digest.DigestUtils

/**
  * @Auther: cxb
  * @Date: 2018/9/6 19:43
  * @Description:
  */
object DeviceIdUtil {

  def getDeviceId(imei:String,idfa:String,wifiMac:String)= {

    val deviceid = if (imei != null && !"".equals(imei)){
      DigestUtils.md5Hex(imei)
    } else if (idfa != null  && !"00000000-0000-0000-0000-000000000000".equals(imei) && !"".equals(idfa)){
      DigestUtils.md5Hex(idfa)
    } else if (wifiMac != null && !"02:00:00:00:00:00".equals(wifiMac) && !"".equals(wifiMac)) {
      DigestUtils.md5Hex(wifiMac)
    } else {
      ""
    }
    deviceid
  }

}
