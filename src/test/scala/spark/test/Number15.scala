package spark.test

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * @Auther: cxb
  * @Date: 2018/8/22 18:53
  * @Description:
  */
object Number15 {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("number10").master("local").getOrCreate()
    val files: Dataset[String] = session.read.textFile("E:\\Desktop\\20180801_data_0.txt\\data_list0000.txt")
    //    val result = files.rdd.flatMap(_.split("\t"))
    //      .map((_, 1)).sortBy(_._2).reduceByKey(_ + _)
    //    println(result.take(10).foreach(f=>println(f+" ")))
    val rowRdd = files.rdd.map { case line =>
      val lines = line.replace("{", "").replace("}", "").split(",")
      val os = lines(0)
      val number = lines(1)
      val time = lines(2)
      val imei = lines(3)
      val model = lines(4)
      val version = lines(5)
      val appPackageName = lines(6)
      val apps: String = lines(7)
     (os, number, time, imei, model, version, appPackageName, apps)
    }
    import session.implicits._
    rowRdd.toDF("os", "number", "time", "imei", "model", "version", "appPackageName", "apps")
      .createTempView("source")
    //val df = session.sql("select distinct a.imei,b.model from source  a,(select imei,model from source) b WHERE a.imei=b.imei")
    val df = session.sql("select imei,model from source")
    df.rdd.coalesce(1).saveAsTextFile("E:\\Desktop\\1027")

//    val files1: Dataset[String] = session.read.textFile("E:\\Desktop\\956\\part-00000")
//    val result = files1.rdd.flatMap((_.split("\t"))).map((_, 1)).reduceByKey(_ + _).sortBy(_._2, true)
//    //.take(10000)
//    result.filter(_._2 >= 2).saveAsTextFile("E:\\Desktop\\0823")
  }
}
//case class User(os: String, number: String, time: String, imei: String, model: String, version: String, appPackageName: String, apps: Array[String])
