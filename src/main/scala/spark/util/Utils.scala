package spark.util

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * @Auther: cxb
  * @Date: 2018/8/22 13:55
  * @Description: 源数据的工具类
  */
object Utils {

  private val logger = Logger.getLogger(Utils.getClass)

  case class User(os: String, number: String, time: String, imei: String, model: String, version: String, appPackageName: String, apps: String)

  def main(args: Array[String]): Unit = {

    val conf=new SparkConf().setAppName("number10").setMaster("local")
    val session = SparkSession.builder().config(conf).getOrCreate()
    //files表示昨天的数据    files1表示今天的数据
    val files: Dataset[String] = session.read.textFile("hdfs://cdh1:9000/checkpoint/tddata")
    import session.implicits._
    val rowRdd = files.rdd.map { case line =>
      val lines = line.replace("{", "").replace("}", "").split(",")
      val os = lines(0)
      val number = lines(1)
      val time = lines(2)
      val imei = lines(3)
      val model = lines(4)
      val version = lines(5)
      val appPackageName = lines(6)
      val apps = lines(7)
      User(os, number, time, imei, model, version, appPackageName, apps)
    }.toDF().createTempView("files1")

    val files1: Dataset[String] = session.read.textFile("hdfs://cdh1:9000/checkpoint/data_list00002")
    val rowRdd1 = files1.rdd.map { case line =>
      val lines = line.replace("{", "").replace("}", "").split(",")
      val os = lines(0)
      val number = lines(1)
      val time = lines(2)
      val imei = lines(3)
      val model = lines(4)
      val version = lines(5)
      val appPackageName = lines(6)
      val apps = lines(7)
      User(os, number, time, imei, model, version, appPackageName, apps)
    }.toDF().createTempView("files2")

    logger.info("----------------------------------------------------------开始执行sql--------------")

    val result = session.sql("select concat(round((select count(distinct files2.imei) from files1 inner join files2 on files1.imei = files2.imei)" +
      "/(select count(distinct files1.imei) from files1)*100,2),'%')")

    logger.info("----------------------------------------------------------执行完毕-----------------")

    result.show()
  }
}
