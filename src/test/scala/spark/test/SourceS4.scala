package spark.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @Auther: cxb
  * @Date: 2018/8/29 09:41
  * @Description:
  */
object SourceS4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SourceS4")
    val sessioon = SparkSession.builder().config(conf).getOrCreate()
    // sessioon.read.textFile("")


  }
}
