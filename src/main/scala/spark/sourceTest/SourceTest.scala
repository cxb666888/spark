package spark.sourceTest

import java.text.SimpleDateFormat
import java.util.Locale

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import spark.util.TimeUtil

/**
  * @Auther: cxb
  * @Date: 2018/8/29 23:05
  * @Description:
  */
case class Person(name: String, age: String, luckNumbers: List[Int])

object SourceTest {


  def main(args: Array[String]): Unit = {

    val tm = "2018-06-03 09:12:16"
    val a = TimeUtil.tranTimeToLong(tm)
    println(a)

  }
}


