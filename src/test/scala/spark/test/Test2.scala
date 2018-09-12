package spark.test

import project.Time1

/**
  * @Auther: cxb
  * @Date: 2018/8/22 09:59
  * @Description:
  */
object Test2 {
  def main(args: Array[String]): Unit = {
    //    val a: Double = 100
    //    val b: Double = 50
    //    val c = b / a
    //    //    println("保留两位：" + f"$c%1.2f") //保留两位  结果:0.50
    //    //    println("保留五位：" + f"$c%1.5f") //保留五位  结果:0.50000
    //    //    println("保留：" + f"$c%06.2f") //保留  结果:000.50
    //    //    println("保留两位另一种写法：" + "%1.2f".format(c))
    //    val result = "%1.2f".format(c)
    //    println(result.isInstanceOf[String])
    //    val results = result.toDouble * 100
    //    println("乘100后的结果:" + results.toInt + "%")

    /**
      * 调用源数据工具类测试
      */
    //val session = SparkSession.builder().appName("Test2").master("spark://cdh1:7077").getOrCreate()
    //import session.implicits._
    //Utils.RowRdd.toDF().createTempView("test2")
    //session.sql("select imei from test2").show()

    println("方法一："+Time1.getCurrent_time())
    println("方法二："+Time1.getCurrent_time())
  }
}
