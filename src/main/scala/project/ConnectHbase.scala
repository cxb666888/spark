//package project
//
//import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
//import org.apache.hadoop.hbase.client.HBaseAdmin
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.spark.{SparkConf, SparkContext}
//
//object ConnectHbase {
//  def main(args: Array[String]): Unit = {
//    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local")
//    val sc = new SparkContext(sparkConf)
//
//    val tablename = "test"
//    val conf = HBaseConfiguration.create()
//    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
//    conf.set("hbase.zookeeper.quorum", "cdh1,cdh2,cdh3")
//    //设置zookeeper连接端口，默认2181
//    conf.set("hbase.zookeeper.property.clientPort", "2181")
//    conf.set(TableInputFormat.INPUT_TABLE, tablename)
//
//    // 如果表不存在则创建表
//    val admin = new HBaseAdmin(conf)
//    if (!admin.isTableAvailable(tablename)) {
//      val tableDesc = new HTableDescriptor(TableName.valueOf(tablename))
//      admin.createTable(tableDesc)
//    }
//    //读取数据并转化成rdd
//    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
//      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
//      classOf[org.apache.hadoop.hbase.client.Result])
//
//    val count = hBaseRDD.count()
//    println(count + "---------------------------------------------------")
//
//    hBaseRDD.foreach { case (_, result) => {
//      //循环得到全部数据
//      //获取行键
//      val key = Bytes.toString(result.getRow)
//      val a = Bytes.toString(result.getValue("cf".getBytes, "a".getBytes))
//      val b = Bytes.toString(result.getValue("cf".getBytes, "b".getBytes))
//      val c = Bytes.toString(result.getValue("cf".getBytes, "c".getBytes))
//      println("Row key:" + key + " a:" + a + " b:" + b + "c" + c)
//      //通过列族和列名获取列
//      //      val name = Bytes.toString(result.getValue("cf".getBytes, "name".getBytes))
//      //      val age = Bytes.toInt(result.getValue("cf".getBytes, "age".getBytes))
//      //      println("Row key:" + key + " name:" + name + " age:" + age)
//    }
//    }
//    println("------------------------查询某条数据---------------------------")
//  }
//
//}
