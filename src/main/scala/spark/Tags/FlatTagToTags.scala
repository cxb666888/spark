package spark.Tags

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import scala.collection.mutable

/**
  * @Auther: cxb
  * @Date: 2018/9/10 11:21
  * @Description:
  */
case class ID(aaid: Option[String], btMac: Option[String], imei: Option[String], serialNo: Option[String], tdid: Option[String], androidId: Option[String], idfa: Option[String], wifiMac: Option[String]) extends Serializable

case class Tags(label: String, name: String, weight: String) extends Serializable

case class DmkTags(deviceid: Option[String], platform: Option[String], offset: Long, id: ID, tags: mutable.Seq[Tags]) extends Serializable


object FlatTagToTags {

  implicit val formats = DefaultFormats

  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    val sc = session.sparkContext

    import session.implicits._
    val ftpPath = "E:\\Desktop\\ftptag.txt"
    val tagPath = "E:\\Desktop\\dmkTag.txt"
    val ftpFrame: DataFrame = session.read.json(ftpPath)
    val dmkFrame: DataFrame = session.read.json(tagPath)

    val result1 = ftpFrame.select("deviceid", "id", "tags")
    val result2 = dmkFrame.select("deviceid", "id", "tags")


    val result3 = result1.union(result2)
    //  result3.coalesce(1).write.mode(SaveMode.Overwrite).json("E:\\Desktop\\outresult3")

    val result4 = dmkFrame.select("platform", "deviceid", "offset")
    //result4.show()
    val result5 = result4.join(result3, "deviceid")

    // result5.coalesce(1).write.mode(SaveMode.Overwrite).json("E:\\Desktop\\outresult5")

    val result6 = result5.rdd.collect().map(line => {
      val platform = line.getAs[String]("platform")
      val deviceid = line.getAs[String]("deviceid")
      val offset = line.getAs[Long]("offset")
      val id = line.getAs[Row]("id")
      // println(id.toString())
      val tags = line.getAs[mutable.Seq[Tags]]("tags")

      //println(tags.toBuffer)
      (deviceid, platform, offset, id, tags)
    })
    // .saveAsTextFile("E:\\Desktop\\result6")

    //  result6.foreach(println)

    val deviceRdd = result5.rdd.map {
      line =>
        val dmkTags: DmkTags = parse(line.toString()).extract[DmkTags]
        (dmkTags.deviceid, (dmkTags, dmkTags.tags))
    }
    //deviceRdd.foreach(println)
    //  deviceRdd.toBuffer
    //.saveAsTextFile("E:\\Desktop\\deviceRdd")


    //    val outPath = ("E:\\Desktop\\ftpTOdmk") + System.currentTimeMillis()
    //    result3.rdd.saveAsTextFile(outPath)
    val rddM = deviceRdd.reduceByKey((x, y) => (x._1, x._2 ++: y._2))
      .map {
        line =>
          val rdd1 = sc.parallelize(line._2._2).map {
            line => (line.label, (line.name, line.weight))
          }.reduceByKey {
            (x, y) => x
          }
          val tuple = (line._1, (line._2._1, rdd1.map(t => (t._1, t._2._1, t._2._2))))
          tuple
      }.foreach(println)

    /* val rddRe = rddM.map{
       line =>
         val list = line._2._2.map {
           line =>
             Tags(line._3, line._2, line._1)
         }.collect().toList
         DmkTags(line._2._1.platform, line._2._1.deviceid, line._2._1.offset, line._2._1.id, list)
     }*/

    /*rddRe.saveAsTextFile("E:\\Desktop\\20180910") */
    sc.stop()
    session.stop()
  }
}