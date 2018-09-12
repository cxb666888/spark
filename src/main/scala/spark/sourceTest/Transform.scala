package spark.sourceTest


import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.{DefaultFormats, _}


/**
  * @Auther: cxb
  * @Date: 2018/8/31 18:35
  * @Description:
  */
case class User(name: String, price: Long)

object Transform {
  implicit val formats = DefaultFormats

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Transform")
      //.setMaster("spark://test:7077")
      .setMaster("local[*]")
    val context = new SparkContext(conf)
    val rdd = context.textFile("E:\\Desktop\\SJson.txt")
    val result = rdd.map {
      line =>
        val p: User = parse(line).extract[User]
        val json =
          (
            ("name" -> p.name) ~
              ("price" -> p.price)
            )
        compact(render(json))
    }
    result.foreach(println)
  }
}
