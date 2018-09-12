package spark.sourceTest


import org.apache.spark.sql.SparkSession
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import spark.util.TimeUtil

object NewTest {
  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()


    val a="2018-06-02 15:28:29"
    println(a.length)

 /*   val path = "E:\\Desktop\\数据格式\\s3原始数据格式\\imei.txt"
    val rdd = session.sparkContext.textFile(path)
    // val rdd=session.read.textFile(path)
    //rdd.foreach(x=>println(x))
    //867182039268923,816718203926893 com.tencent.mtt,bubei.tingshu,com.jingdong.app.mall,com.snda.wifilocating,com.yixia.videoeditor,com.tencent.mm,landai.netgame.pqmjsy.activity,com.tencent.qqlive,com.zhongcai500
    val cc = List(1)


    var imei = ""

    val result = rdd.map(x => {
      val str = new StringBuffer()
      val sz = x.replaceAll(",", "-")
      imei = sz.split(" ")(0)
      val apps = sz.split(" ")(1)
      val apps_result = apps.replaceAll("-", ",")
      if (imei.contains("-")) {
        val s = imei.split("-")
        val a = s.map(x => (x -> apps))
        val c = a.map(x => (x._1 + "," + x._2.replaceAll("-", ",") + "\n")).toList
        // c.foreach(print)

   /*     val json =
          c.map {
            c =>
              (
                ("imei" -> imei) ~
                  ("packName" -> c.toBuffer)
                )
          }
        compact(render(json))*/


      } else {
        s"${imei},${apps_result}"
      }


    }).collect

    result.foreach(x => print(x))

*/
  }
}