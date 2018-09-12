package spark.sourceTest

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Auther: cxb
  * @Date: 2018/8/30 18:14
  * @Description:
  */
object TestImei {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestImei")
      //.setMaster("spark://test:7077")
      .setMaster("local[*]")
    val context = new SparkContext(conf)
    val rdd = context.textFile("E:\\Desktop\\数据格式\\s3原始数据格式\\imei.txt")
    //    val strings = "a0000038db0c302 com.yunlian.wewe,com.octinn.birthdayplus com.elinkway.infinitemovies"
    //    val a = strings.replace(" ", "-")
    //    val b = a.indexOf("-") //15
    //    println(b)
    //
    //    println(result)
    //    val func: Array[String] = strings.split(a.substring(b, b + 1))
    //    func.foreach(println)
    //    println(func(0))
    val strings = "a0000038db0c302,111 com.yunlian.wewe,com.octinn.birthdayplus com.elinkway.infinitemovies"

    val index111 = strings.indexOf(" ")
    println("index111:" + index111)
    val result111 = strings.substring(0, index111)
    val result2111 = strings.substring(result111.length)
    println("result111:" + result111 + "result2111:" + result2111)
    val re = result2111.replace(" ", ",")
    println("re:" + re)
    val lasts = re.substring(1, re.length)
    println("lastslastslasts:" + lasts)
//
//    val a = strings.replace(" ", "-")
//    println("a:" + a)
//    val b = a.indexOf("-") //15
//    println("b:" + b)
//    val result = a.substring(0, b)
//    println("result:" + result)
//    val result2 = a.substring(b, a.length)
//    val replace2 = result2.replace("-", ",")
//    val result3 = replace2.substring(1, replace2.length)
//    println("result3:" + result3)
//
//
//    val func: Array[String] = a.split(a.substring(a.indexOf("-"), a.indexOf("-") + 1))
//    func.foreach(f => println("全部:" + f))
//    println("func(0):" + func(0))
//    println("func(1):" + func(1))
//    println("func(2):" + func(2))
    /*    val result = rdd.collect.map {
          t =>
            val index = t.toString.indexOf("\t")
    //        val lines = t.split(index)
    //        val imei = lines(0)
    //        val packName = lines(1)
            println(index)
        }*/
    //result.collect()
    context.stop()
  }
}
