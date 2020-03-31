package com.pengzhaopeng.qz.controller

import com.pengzhaopeng.qz.service.EtlDataService
import com.pengzhaopeng.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @author 17688700269
  * @date 2020/3/31 15:51
  * @Version 1.0
  * @description
  */
object DwdController {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val sparkConf = new SparkConf().setAppName("dwd_qz_controller").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
    HiveUtil.openCompression(sparkSession) //开启压缩
    HiveUtil.useSnappyCompression(sparkSession) //使用snappy压缩
    EtlDataService.etlQzChapter(ssc, sparkSession)
    EtlDataService.etlQzChapterList(ssc, sparkSession)
    EtlDataService.etlQzPoint(ssc, sparkSession)
    EtlDataService.etlQzPointQuestion(ssc, sparkSession)
    EtlDataService.etlQzSiteCourse(ssc, sparkSession)
    EtlDataService.etlQzCourse(ssc, sparkSession)
    EtlDataService.etlQzCourseEdusubject(ssc, sparkSession)
    EtlDataService.etlQzWebsite(ssc, sparkSession)
    EtlDataService.etlQzMajor(ssc, sparkSession)
    EtlDataService.etlQzBusiness(ssc, sparkSession)
    EtlDataService.etlQzPaperView(ssc, sparkSession)
    EtlDataService.etlQzCenterPaper(ssc, sparkSession)
    EtlDataService.etlQzPaper(ssc, sparkSession)
    EtlDataService.etlQzCenter(ssc, sparkSession)
    EtlDataService.etlQzQuestion(ssc, sparkSession)
    EtlDataService.etlQzQuestionType(ssc, sparkSession)
    EtlDataService.etlQzMemberPaperQuestion(ssc, sparkSession)
  }
}
