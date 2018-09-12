package spark.sourceTest

import org.apache.commons.codec.digest.DigestUtils
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.{DefaultFormats, _}


/**
  * @Auther: cxb
  * @Date: 2018/9/4 12:06
  * @Description:
  */
case class SourceApp4(name: String, packageName: List[String], ver: String) extends Serializable

object Source4 {
  implicit val formats = DefaultFormats

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("dmk").setMaster("local[*]")
    val context = new SparkContext(conf)

    val path = "C:\\Users\\passonly\\Desktop\\yuanshi\\i\\s4.txt"

    val rdd = context.textFile(path)

    rdd.map{
      line=>
        line.split(",")

}
    }
  }


