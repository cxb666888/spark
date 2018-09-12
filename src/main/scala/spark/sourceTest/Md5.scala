package spark.sourceTest

import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Auther: cxb
  * @Date: 2018/8/31 13:29
  * @Description:
  */
object Md5 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Md5")
      //.setMaster("spark://test:7077")
      .setMaster("local[*]")
    val context = new SparkContext(conf)
    val rdd = context.textFile("E:\\Desktop\\数据格式\\s3原始数据格式\\s4.txt")
    val rddReturn = rdd.collect.map {
      line =>
        //val p: SourceA4 = parse(line).extract[SourceA4]
        val user = line.split(",")
        val falg = user(0)
        val pkgnName = user(1)
        val receiveDate = user(2)

        var mac = ""
        var imei = ""
        var dericeid = ""

        if (falg.contains("mac")) {
          mac = falg
          dericeid = DigestUtils.md5Hex(mac)
        } else {
          imei = falg
          dericeid = DigestUtils.md5Hex(imei)
        }

        println("mac:" + mac)
        println("imei:" + imei)

val a=""

    }




  }
}
