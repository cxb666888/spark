package spark.test

import com.alibaba.fastjson.JSON
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


/**
  * @Auther: cxb
  * @Date: 2018/8/24 16:33
  * @Description:
  */
object Test4 {

  private val logger = Logger.getLogger(Test4.getClass)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Test4")
      //.setMaster("spark://192.168.118.144:7077")
      .setMaster("local")

    val sc = new SparkContext(conf)
    val session = SparkSession.builder().config(conf).getOrCreate()
    session.conf.set("spark.sql.broadcastTimeout", 1200)
    session.conf.set("spark.sql.crossJoin.enabled", "true")

    import session.implicits._
    val files = sc.textFile("E:\\Desktop\\20180801_data_0.txt\\data_list0000.txt")

    val info = files.map(x => {
      val os = JSON.parseObject(x).getString("os")
      val serialNumber = JSON.parseObject(x).getString("serialNumber")
      val time = JSON.parseObject(x).getString("time")
      val imei = JSON.parseObject(x).getString("imei")
      val model = JSON.parseObject(x).getString("model")
      val sourceAppVersion = JSON.parseObject(x).getString("sourceAppVersion")
      val sourceAppPackageName = JSON.parseObject(x).getString("sourceAppPackageName")
      val apps = JSON.parseObject(x).getString("apps")
      (os, serialNumber, time, imei, model, sourceAppVersion, sourceAppPackageName, apps)
    })

    val ls = info.map(x => {
      val os = x._1
      val serialNumber = x._2
      val time = x._3
      val imei = x._4
      val model = x._5
      val sourceAppVersion = x._6
      val sourceAppPackageName = x._7
      val apps = x._8.replace(",", "-")
      (os, serialNumber, time, imei, model, sourceAppVersion, sourceAppPackageName, apps)
    }).toDF("os", "serialNumber", "time", "imei", "model", "sourceAppVersion", "sourceAppPackageName", "apps")

    ls.createTempView("files1")

    //---------------------------------------------------------------------------
    val files1 = sc.textFile("E:\\Desktop\\20180801_data_0.txt\\data_list0000 (2).txt")

    val info1 = files1.map(x => {
      val os = JSON.parseObject(x).getString("os")
      val serialNumber = JSON.parseObject(x).getString("serialNumber")
      val time = JSON.parseObject(x).getString("time")
      val imei = JSON.parseObject(x).getString("imei")
      val model = JSON.parseObject(x).getString("model")
      val sourceAppVersion = JSON.parseObject(x).getString("sourceAppVersion")
      val sourceAppPackageName = JSON.parseObject(x).getString("sourceAppPackageName")
      val apps = JSON.parseObject(x).getString("apps")
      (os, serialNumber, time, imei, model, sourceAppVersion, sourceAppPackageName, apps)
    })

    val ls1 = info1.map(x => {
      val os = x._1
      val serialNumber = x._2
      val time = x._3
      val imei = x._4
      val model = x._5
      val sourceAppVersion = x._6
      val sourceAppPackageName = x._7
      val apps = x._8.replace(",", "-")
      (os, serialNumber, time, imei, model, sourceAppVersion, sourceAppPackageName, apps)
    }).toDF("os", "serialNumber", "time", "imei", "model", "sourceAppVersion", "sourceAppPackageName", "apps")
    ls1.createTempView("files2")

    //session.sql("select apps from files1").take(10).foreach(f => println(f))
    val result = session.sql("select concat(round((select count(distinct files2.imei) from files1 inner join files2 on files1.imei = files2.imei)" +
      "/(select count(distinct files1.imei) from files1)*100,2),'%') result")
    result.show()
    // result.rdd.saveAsTextFile("hdfs://test:9000/checkpoint/0826")
    //    val ps = new Properties()
    //    ps.put("user", "root")
    //    ps.put("password", "123456")
    //    //ps.put("dbtable", "0810cxb")
    //    logger.info("----------------------------------------------------------------准备写入")
    //    result.write.mode("Append").jdbc(url = "jdbc:mysql://192.168.118.144:3306/sparkTest?useUnicode=true&characterEncoding=gbk", "0810cxb0824", ps)
    //    logger.info("----------------------------------------------------------------写入成功")
  }
}