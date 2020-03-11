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
//      .setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder()
      .config(conf)
      .config("spark.sql.orc.impl", "native")
      .config("spark.sql.orc.enableVectorizedReader", true)
      .enableHiveSupport()
      .getOrCreate()

    //创建临时表，目的是防止分析完的数据直接导入 MySQL 失败那就白分析了

    //1、课程学习反馈指标
//    appCourseStudyAnalysis(day, spark)11

    //2、各系统版本访问统计
    appVersionAnalysis(day,spark)

    //停止
    spark.stop()
  }

  /**
    * 各系统版本访问统计
    * @param day
    * @param spark
    * @return
    */
  private def appVersionAnalysis(day: String, spark: SparkSession) = {
    //建表
    spark.sql(
      s"""
         |create table if not exists education_online.tmp_app_version_analysis_${day}(
         |	os string,
         |	version string,
         |	access_count int,
         |	dt int
         |)row format delimited fields terminated by '\t'
         |location '/warehouse/education_online/tmp/tmp_app_version_analysis_${day}'
       """.stripMargin)
    //导入表
    spark.sql(
      s"""
         |insert overwrite table education_online.tmp_app_version_analysis_${day}
         |select
         |	os,
         |	version,
         |	count(1) access_count,
         |	dt
         |from education_online.user_behavior
         |where dt=${day}
         |group by os,version,dt
       """.stripMargin)
  }

  /**
    * 课程学习反馈指标
    * @param day 日期
    * @param spark
    * @return
    */
  private def appCourseStudyAnalysis(day: String, spark: SparkSession) = {
    //建表
    spark.sql(
      s"""
         |drop table if exists education_online.tmp_app_course_study_analysis_${day}
       """.stripMargin)
    spark.sql(
      s"""
         |create table if not exists education_online.tmp_app_course_study_analysis_${day}(
         |    watch_video_count INT,
         |    complete_video_count INT,
         |    dt INT
         |)row format delimited fields terminated by '\t'
         |location '/warehouse/education_online/tmp/tmp_app_course_study_analysis_${day}'
           """.stripMargin)

    //将分析结果导入临时表
    spark.sql(
      s"""
         |insert overwrite table education_online.tmp_app_course_study_analysis_${day}
         |select
         |	sum(watch_video_count),
         |	sum(complete_video_count),
         |	dt
         |from(
         |		select
         |			count(distinct uid) as watch_video_count,
         |			0 as complete_video_count,
         |			dt
         |		from education_online.user_behavior
         |		where dt=${day} and event_key="startVideo"
         |		group by dt
         |		union all
         |		select
         |			0 as watch_video_count,
         |			count(distinct uid) as complete_video_count,
         |			dt
         |		from education_online.user_behavior
         |		where dt=${day} and event_key="endVideo"
         |		and (end_video_time-start_video_time)>=video_length
         |		group by dt
         |	)t1
         |group by dt
       """.stripMargin)

  }
}
