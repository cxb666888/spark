package spark.test

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * @Auther: cxb
  * @Date: 2018/8/21 21:56
  * @Description:
  */
object Number10 {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("number10").master("local").getOrCreate()
    val files: Dataset[String] = session.read.textFile("E:\\Desktop\\20180801_data_0.txt\\data_list0000.txt")
    val rowRdd = files.rdd.map{case line =>
      val lines = line.replace("{", "").replace("}","").split(",")
      val os = lines(0)
      val number = lines(1)
      val time = lines(2)
      val imei = lines(3)
      val model = lines(4)
      val version = lines(5)
      val appPackageName = lines(6)
      val apps = lines(7)
      ( os,number, time, imei, model, version, appPackageName, apps)
    }
    import session.implicits._
    rowRdd.toDF("os", "number", "time", "imei","model", "version", "appPackageName", "apps")
      .createTempView("source")
//    val df: DataFrame = session.sql("select distinct model  from source")
//    println("总设备去重后为:"+df.rdd.count())//总设备没去重为:100048
//    val df1: DataFrame = session.sql("select  model  from source")
//    println("总设备没去重为:"+df1.rdd.count())//总设备去重后为:3277
//    val df2: DataFrame = session.sql("select  *  from source")
//    println("总的数据条数为:"+df2.rdd.count())
    val df3: DataFrame = session.sql("select  distinct model  from source")
    df3.rdd.coalesce(1).saveAsTextFile("E:\\Desktop\\00821")
    //println("手机类型为:"+df3.rdd.count())
  }
}
