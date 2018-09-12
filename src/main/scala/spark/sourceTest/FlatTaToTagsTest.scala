package spark.sourceTest

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * @Auther: cxb
  * @Date: 2018/9/11 11:20
  * @Description:
  */
object FlatTaToTagsTest {
  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    val sc = session.sparkContext

    val arr = sc.parallelize(Seq(("A", 1), ("B", 2), ("C", 3)))
    arr.map {
      line =>
        line._1 + line._2
    }.foreach(println)

  }
}
