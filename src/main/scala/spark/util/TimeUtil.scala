package spark.util

import java.text.SimpleDateFormat

/**
  * @Auther: cxb
  * @Date: 2018/9/5 20:41
  * @Description: 将日期转换为时间戳
  */
object TimeUtil {
  def tranTimeToLong(tm: String): Long = {
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dt = fm.parse(tm)
    val aa = fm.format(dt)
    val tim: Long = dt.getTime()
    tim
  }

}
