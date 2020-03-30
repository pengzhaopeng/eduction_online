package com.pengzhaopeng.member.controller

import com.pengzhaopeng.member.service.ETLDataService
import com.pengzhaopeng.util.HiveUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * @author 17688700269
  * @date 2020/3/26 17:34
  * @Version 1.0
  * @description
  */
object DwdMemberController {
  def main(args: Array[String]): Unit = {
    //设置系统属性 比如操作hdfs的用户，这样方便全局取
    System.setProperty("HADOOP_USER_NAME", "dog")

    //设置hive参数
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("dwd_member_import")
      .setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc: SparkContext = sparkSession.sparkContext
    //开启动态分区
    HiveUtil.openDynamicPartition(sparkSession)
    //开启压缩
    HiveUtil.openCompression(sparkSession)
    //使用 snappy 压缩
    HiveUtil.useSnappyCompression(sparkSession)

    //清洗数据并导入相应的dwd层
    //导入基础广告表数据
    ETLDataService.etlBaseAdLog(ssc,sparkSession)

    //导入基础网站表数据
    ETLDataService.etlBaseWebSiteLog(ssc, sparkSession)

    //清洗用户数据
    ETLDataService.etlMemberLog(ssc, sparkSession)

    //清洗用户注册数据
    ETLDataService.etlMemberRegtypeLog(ssc, sparkSession)

    //导入用户支付情况记录
    ETLDataService.etlMemPayMoneyLog(ssc, sparkSession)

    //导入vip基础数据
    ETLDataService.etlMemVipLevelLog(ssc, sparkSession)
  }
}
