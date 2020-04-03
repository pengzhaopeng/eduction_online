package com.pengzhaopeng.member.controller

import com.pengzhaopeng.member.service.DwsMemberService
import com.pengzhaopeng.member.service.DwsMemberService.importMemberUseSql
import com.pengzhaopeng.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @author 17688700269
  * @date 2020/3/30 15:46
  * @Version 1.0
  * @description
  */
object DwsController {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "dog")
    val sparkConf = new SparkConf().setAppName("dws_member_import")
      .setMaster("local[*]")
      .set("spark.sql.autoBroadcastJoinThreshold", "104857600") //100m
//      .getOption("")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
    HiveUtil.openCompression(sparkSession) //开启压缩
    HiveUtil.useSnappyCompression(sparkSession) //使用snappy压缩
//    DwsMemberService.importMember(sparkSession, "20190722") //根据用户信息聚合用户表数据
    //    DwsMemberService.importMemberUseApi(sparkSession, "20190722")

    importMemberUseSql(sparkSession, "20190722")
  }
}
