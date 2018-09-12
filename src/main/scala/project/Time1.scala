package project

import java.text.SimpleDateFormat
import java.util.Date

/**
  * @Auther: cxb
  * @Date: 2018/8/22 18:14
  * @Description:
  */
object Time1 {
    val now = new Date()
    def getCurrent_time(): Long = {
      val a = now.getTime
      var str = a+""
      str.substring(0,10).toLong
    }
    def getZero_time():Long={
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val a = dateFormat.parse(dateFormat.format(now)).getTime
      var str = a+""
      str.substring(0,10).toLong
    }

}
