package spark.test

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * @Auther: cxb
  * @Date: 2018/8/27 14:05
  * @Description: 读取s1文件转成parquet文件存储在hdfs上
  */
object Test6 {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("test3").master("local").getOrCreate()
    val files: Dataset[String] = session.read.textFile("E:\\Desktop\\one.txt")
    //files.coll
  }
}
