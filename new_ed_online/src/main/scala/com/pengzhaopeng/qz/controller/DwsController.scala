package com.pengzhaopeng.qz.controller

import com.pengzhaopeng.qz.service.DwsQzService
import com.pengzhaopeng.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @author 17688700269
  * @date 2020/3/31 17:10
  * @Version 1.0
  * @description
  */
object DwsController {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "dog")
    val sparkConf = new SparkConf().setAppName("dws_qz_controller")
//      .setMaster("local[*]")

    //设置广播的表
    sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "104857600") //100m

    val sparkSession = SparkSession.builder()
      .config(sparkConf)
      .config("spark.debug.maxToStringFields", "200")
      .enableHiveSupport().getOrCreate()

    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    ssc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
    HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
    HiveUtil.openCompression(sparkSession) //开启压缩
    HiveUtil.useSnappyCompression(sparkSession) //使用snappy压缩
    val dt = "20190722"
//    DwsQzService.saveDwsQzChapter(sparkSession, dt)
//    DwsQzService.saveDwsQzCourse(sparkSession, dt)
//    DwsQzService.saveDwsQzMajor(sparkSession, dt)
//    DwsQzService.saveDwsQzPaper(sparkSession, dt)
//    DwsQzService.saveDwsQzQuestion(sparkSession, dt)
    DwsQzService.saveDwsUserPaperDetail(sparkSession, dt)

    while (true){
      Thread.sleep(5000)
      println("---")
    }
  }
}
