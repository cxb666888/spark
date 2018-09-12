package spark.test

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

/**
  * @Auther: cxb
  * @Date: 2018/8/23 10:36
  * @Description:
  */
object Test3 {

  case class User(location: String, model: String, apps: String)

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local").setAppName("test3")
    val spark = new SparkContext(conf)
    val sqlContext = new SQLContext(spark)
    import sqlContext.implicits._
    val data: RDD[String] = sqlContext.sparkContext.textFile("E:\\Desktop\\one.txt")
    val info = data.map(x => {
      val apps = JSON.parseObject(x).getString("apps")
      val model = JSON.parseObject(x).getString("model")
      val location = JSON.parseObject(x).getString("location")
      User(location, model, apps)
    }).toDF().as[User]
    info.rdd.saveAsTextFile("e:\\desktop\\1908")

    //    val df=info.toDF("imei", "model", "apps")
    //    df.rdd.saveAsTextFile("E:\\Desktop\\2339")
//    val ls = info.map(x => {
//      val apps = x.apps.replace(",", "-")
//      //println(apps)
//      val model = x.model
//      val location = x.location
//      User(location, model, apps)
//    }).toDF().as[User]
    //val df: DataFrame = ls.toDF("imei", "model", "apps")
    //ls.rdd.coalesce(1).saveAsTextFile("hdfs://test:9000/checkpoint/20180827")
    //ls.write.format("parquet").option("delimiter","\t").save("hdfs://test:9000/checkpoint/20180827")
    //ls.show()
    //    //--------------------------------------------------------------------------
    //    //        ls.createTempView("s")
    //    //        val result: DataFrame = sqlContext.sql("select imei,model,apps from s")
    //    //        result.rdd.saveAsTextFile("E:\\Desktop\\1005")
    //    //--------------------------------------------------------------------------
    //    val files2 = spark.textFile("E:\\Desktop\\2343\\part-00000")
    //    val counts = files2.map { line =>
    //      val lines = line.split(",")
    //      val imei = lines(0)
    //      val model = lines(1)
    //      val apps = lines(2)
    //      (imei, model, apps)
    //    }.groupBy((_._1))
    //    //.saveAsTextFile("E:\\Desktop\\1127")
    //    println("共有-->" + counts.count() + "条重复") //96286
    //    //--------------------------------------------------------------------
    //

  }
}