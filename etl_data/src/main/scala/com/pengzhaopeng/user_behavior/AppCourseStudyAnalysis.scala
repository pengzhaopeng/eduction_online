package com.pengzhaopeng.user_behavior

import com.pengzhaopeng.utils.StringUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @author 17688700269
  * @date 2020/3/11 14:21
  * @Version 1.0
  * @description 课程指标反馈统计
  */
object AppCourseStudyAnalysis {
  def main(args: Array[String]): Unit = {

    //验证参数-日期
    val day = args(0)
    if (StringUtil.isEmpty(day) || day.length != 8) {
      println("Usage: Please input date, eg:20190413")
      System.exit(1)
    }

    //获取SparkSession,并支持Hive操作
    val conf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    //创建临时表，目的是防止分析完的数据直接导入 MySQL 失败那就白分析了
    spark.sql(
      s"""
         |drop table if exists "education_online".education_online.tmp_app_course_study_analysis_${day};
         |create table if not exists education_online.tmp_app_course_study_analysis_${day}(
         |    watch_video_count INT,
         |    complete_video_count INT,
         |    dt INT
         |) row format delimited fields terminated by "\t" stored as ORC
         |location '/warehouse/education_online/tmp/tmp_app_course_study_analysis_${day}';
       """.stripMargin)

    //将分析结果导入临时表


    //停止
    spark.stop()
  }
}
