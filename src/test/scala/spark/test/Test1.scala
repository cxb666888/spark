package spark.test

import java.util.Properties

import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}
//import spark.util.ConnectJDBC
//import spark.util.ConnectJDBC.ps

/**
  * @Auther: cxb
  * @Date: 2018/8/21 23:35
  * @Description: 需求11 计算数据源的重复率
  */
object Test1 {
  private val logger = Logger.getLogger(Test1.getClass)

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("number10").master("local").getOrCreate()
    val files1: Dataset[String] = session.read.textFile("E:\\Desktop\\1.txt")
    import session.implicits._


    val user1 = files1.rdd.map {
      line =>
        val lines = line.split(",")
        (lines(0).toInt, lines(1), lines(2))
    }.toDF("id", "name", "sex")
    user1.createTempView("files1")

    val files2: Dataset[String] = session.read.textFile("E:\\Desktop\\2.txt")
    val user2 = files2.rdd.map {
      line =>
        val lines = line.split(",")
        (lines(0).toInt, lines(1), lines(2))
    }.toDF("id", "name", "sex")
    user2.createTempView("files2")

    val result = session.sql("select concat(round((select count(distinct files2.id) from files1 inner join files2 on files1.id = files2.id)" +
      "/(select count(distinct files1.id) from files1)*100,2),'%') result")
    // result.rdd.coalesce(1).saveAsTextFile("e:\\Desktop\\3ss")
    //result.show()

    val ps = new Properties()
    ps.put("user", "root")
    ps.put("password", "123456")
    //ps.put("dbtable", "0810cxb")
    logger.info("----------------------------------------------------------------准备写入")
    result.write.mode("Append").jdbc(url = "jdbc:mysql://192.168.118.130:3306/sparkTest?useUnicode=true&characterEncoding=gbk&zeroDateTimeBehavior=convertToNull", "0810cxb0824", ps)
    logger.info("----------------------------------------------------------------写入成功")
    //println(result.collect().toString)
   // ConnectJDBC.updates("update 0810cxb c set c.name= '" + result.show().toString + "'")
  }
}
