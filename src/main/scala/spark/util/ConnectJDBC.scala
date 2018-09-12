//package spark.util
//
//import java.sql.{Connection, DriverManager}
//import java.util.Properties
//
//import org.apache.log4j.Logger
//
///**
//  * @Auther: cxb
//  * @Date: 2018/8/24 14:27
//  * @Description:
//  */
//object ConnectJDBC {
//
//  private val driver = "com.mysql.jdbc.Driver"
//  private val url = "jdbc:mysql://192.168.118.130:3306/sparkTest?useUnicode=true&characterEncoding=gbk&zeroDateTimeBehavior=convertToNull"
//  private var connection: Connection = null
//  private val logger = Logger.getLogger(ConnectJDBC.getClass)
//  private val ps = new Properties()
//  ps.put("user", "root")
//  ps.put("password", "123456")
//  ps.put("dbtable", "0810cxb")
//
//  /**
//    * 测试
//    *
//    * @param args
//    */
//  def main(args: Array[String]): Unit = {
//    var addiName = "1243"
//    this.updates("update 0810cxb c set c.name= '" + addiName + "' where c.id=1")
//  }
//
//  /**
//    * 更新数据库
//    *
//    * @param sql
//    */
//  def updates(sql: String): Unit = {
//    try {
//      Class.forName(driver)
//      connection = DriverManager.getConnection(url, ps)
//      val statement = connection.createStatement()
//      logger.info("连接成功！！准备执行sql")
//      val resultSet = statement.executeUpdate(sql)
//      logger.info("插入成功！！！！！！！")
//    } catch {
//      case e => e.printStackTrace()
//    } finally {
//      if (null != connection) {
//        connection.close()
//      }
//    }
//  }
//}
