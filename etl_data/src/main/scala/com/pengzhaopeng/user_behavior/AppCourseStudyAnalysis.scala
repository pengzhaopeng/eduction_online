package com.pengzhaopeng.user_behavior

import com.pengzhaopeng.utils.StringUtil
import org.apache.spark.{SparkConf, SparkContext}
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
    var startDay = args(1)
    var endDay = args(2)
    if (StringUtil.isEmpty(day) || day.length != 8) {
      println("Usage: Please input date, eg:${day}")
      System.exit(1)
    }
    if (StringUtil.isEmpty(startDay) || day.length != 8) {
      startDay = String.valueOf(20190413)
    }
    if (StringUtil.isEmpty(endDay) || day.length != 8) {
      endDay = String.valueOf(20190419)
    }

    //获取SparkSession,并支持Hive操作
    val conf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder()
      .config(conf)
      .config("spark.sql.orc.impl", "native")
      .config("spark.sql.orc.enableVectorizedReader", value = true)
      .config("spark.sql.shuffle.partitions", "10")
      .enableHiveSupport()
      .getOrCreate()

    val ssc: SparkContext = spark.sparkContext
//    ssc.hadoopConfiguration.set("fs.defaultFS","hdfs://nameservice1")
//    ssc.hadoopConfiguration.set("dfs.nameservices","nameservice1")

    //创建临时表，目的是防止分析完的数据直接导入 MySQL 失败那就白分析了

    //测试
    //    appTest(spark)

    //1、课程学习反馈指标
//        appCourseStudyAnalysis(day, spark)

    //2、各系统版本访问统计
//        appVersionAnalysis(day,spark)

    //3、渠道新用户统计
//    appChannelAnalysis(day, spark)

    //4、访问次数分布（我们的指标是1-2次(包含)，3-4次(包含)，大于4次）
//    appAccessCountRangerAnalysis(day, spark)

    //5、漏斗分析
//    appStudyFunnelAnalysis(day,spark)

    //6、7日留存分析
    appSevenDaysRetainedAnalysis(startDay,endDay,spark)

    while (true) {
//      println(".....1")
    }

//    //停止
//    spark.stop()


  }

  /**
    *
    * @param startDay 开始日期
    * @param endDay 结束日期 （统计日期）
    * @param spark
    */
  def appSevenDaysRetainedAnalysis( startDay: String, endDay: String, spark: SparkSession) = {
    //创建临时结果表
    spark.sql(
      s"""
         |drop table if exists education_online.tmp_seven_days_retained_analysis_${endDay}
       """.stripMargin)
    spark.sql(
      s"""
         |create table education_online.tmp_seven_days_retained_analysis_${endDay}(
         |    register_day INT,
         |    zero_interval_retained_rate DOUBLE,
         |    one_interval_retained_rate DOUBLE,
         |    two_interval_retained_rate DOUBLE,
         |    three_interval_retained_rate DOUBLE,
         |    four_interval_retained_rate DOUBLE,
         |    five_interval_retained_rate DOUBLE,
         |    six_interval_retained_rate DOUBLE,
         |    dt INT
         |) row format delimited fields terminated by "\t"
       """.stripMargin)

    //生成用户留存表
    spark.sql(
      s"""
         |drop table if exists education_online.tmp_user_retained_${startDay}_${endDay}
       """.stripMargin)
    spark.sql(
      s"""
         |create table if not exists education_online.tmp_user_retained_${startDay}_${endDay}
         |as
         |select
         |	register_day,
         |	day_interval,
         |	count(1) as retained
         |from
         |(
         |	select
         |		t1.uid,
         |		t1.register_day,
         |		t2.active_day,
         |		datediff(from_unixtime(cast(t2.event_time as int),"yyyy-MM-dd"),from_unixtime(cast(t1.event_time as int),"yyyy-MM-dd")) as day_interval
         |	from
         |	(
         |		select
         |			uid,
         |			dt as register_day,
         |			event_time
         |		from education_online.user_behavior
         |		where dt between ${startDay} and ${endDay}
         |		and event_key = "registerAccount"
         |	)t1
         |	left join
         |	(
         |		select
         |			uid,
         |			dt as active_day,
         |			max(event_time) as event_time
         |		from education_online.user_behavior
         |		where dt between ${startDay} and ${endDay}
         |		group by uid,dt
         |	)t2
         |on t1.uid = t2.uid
         |)t3
         |where day_interval>=0
         |group by register_day,day_interval
       """.stripMargin)

    //生成用户留存率表
    spark.sql(
      s"""
         |drop table if exists education_online.tmp_user_retained_rate_${startDay}_${endDay}
       """.stripMargin)
    spark.sql(
      s"""
         |create table if not exists education_online.tmp_user_retained_rate_${startDay}_${endDay}
         |as
         |select
         |	register_day,
         |	day_interval,
         |	retained,
         |	round(retained/register_count,4) as retained_rate,
         |	current_dt
         |from(
         |	select
         |		t1.register_day,
         |		t1.day_interval,
         |		t1.retained,
         |		t2.register_count,
         |		${endDay} as current_dt
         |	from
         |		(
         |			select
         |				register_day,
         |				day_interval,
         |				retained
         |			from education_online.tmp_user_retained_${startDay}_${endDay}
         |		)t1
         |		left join
         |		(
         |			select
         |				dt,
         |				count(1) as register_count
         |			from education_online.user_behavior
         |			where dt between ${startDay} and ${endDay}
         |			and event_key="registerAccount"
         |			group by dt
         |		)t2
         |		on t1.register_day = t2.dt
         |		group by t1.register_day,t1.day_interval,t1.retained,t2.register_count
         |) t3
       """.stripMargin)

    //列转行
    spark.sql(
      s"""
         |insert overwrite table education_online.tmp_seven_days_retained_analysis_${endDay}
         |select
         |	register_day,
         |	max(case when day_interval = 0 then retained_rate else 0 end) as zero_interval_retained_rate,
         |	max(case when day_interval = 1 then retained_rate else 0 end) as one_interval_retained_rate,
         |	max(case when day_interval = 2 then retained_rate else 0 end) as two_interval_retained_rate,
         |	max(case when day_interval = 3 then retained_rate else 0 end) as three_interval_retained_rate,
         |	max(case when day_interval = 4 then retained_rate else 0 end) as four_interval_retained_rate,
         |	max(case when day_interval = 5 then retained_rate else 0 end) as five_interval_retained_rate,
         |	max(case when day_interval = 6 then retained_rate else 0 end) as six_interval_retained_rate,
         |	current_dt
         |from education_online.tmp_user_retained_rate_${startDay}_${endDay}
         |group by register_day,current_dt
       """.stripMargin)

  }

  /**
    * 漏斗分析
    * @param day
    * @param spark
    * @return
    */
  def appStudyFunnelAnalysis(day: String, spark: SparkSession) = {
//    spark.sql(
//      s"""
//         |drop table if exists user_behavior.app_study_funnel_analysis_${day}
//       """.stripMargin)
    spark.sql(
      s"""
         |create table if not exists education_online.tmp_app_study_funnel_analysis_${day}(
         |	start_app_count int,
         |	start_video_count int,
         |	complete_video_count int,
         |	start_homework_count int,
         |	complete_homework_count int,
         |	dt int
         |)row format delimited fields terminated by "\t"
         |location '/warehouse/education_online/tmp/app_study_funnel_analysis_${day}'
       """.stripMargin)
    spark.sql(
      s"""
         |insert overwrite table education_online.tmp_app_study_funnel_analysis_${day}
         |select
         |	count(DISTINCT t1.uid) as start_app_count,
         |	count(distinct t2.uid) as start_video_count,
         |	count(distinct t3.uid) as complete_video_count,
         |	count(distinct t4.uid) as start_homework_count,
         |	count(distinct t5.uid) as complete_homework_count,
         |	dt
         |from
         |(
         |	select
         |		uid,dt
         |	from education_online.user_behavior
         |	where dt=${day}
         |	and event_key="startApp"
         |)t1
         |left join
         |(
         |	select
         |		uid
         |	from education_online.user_behavior
         |	where dt=${day}
         |	and event_key="startVideo"
         |)t2
         |on t1.uid=t2.uid
         |left join
         |(
         |	select
         |		uid
         |	from education_online.user_behavior
         |	where dt=${day}
         |	and event_key="endVideo"
         |	and (end_video_time-start_video_time)>=video_length
         |)t3
         |on t2.uid=t3.uid
         |left join
         |(
         |	select
         |		uid
         |	from education_online.user_behavior
         |	where dt=${day}
         |	and event_key="startHomework"
         |)t4
         |on t3.uid=t4.uid
         |left join
         |(
         |	select
         |		uid
         |	from education_online.user_behavior
         |	where dt=${day}
         |	and event_key="completeHomework"
         |)t5
         |on t4.uid=t5.uid
         |GROUP BY t1.dt
       """.stripMargin)
  }

  /**
    * 问次数分布（我们的指标是1-2次(包含)，3-4次(包含)，大于4次）
    *
    * @param day
    * @param spark
    */
  def appAccessCountRangerAnalysis(day: String, spark: SparkSession) = {
    //创建次数分布表
    spark.sql(
      s"""
         |create table if not exists education_online.tmp_app_access_count_ranger_analysis_${day}(
         |    le_two INT,
         |    le_four INT,
         |    gt_four INT,
         |    dt INT
         |) row format delimited fields terminated by "\t"
         |location '/warehouse/education_online/tmp/tmp_app_access_count_ranger_analysis_${day}'
       """.stripMargin)

    //先求用户访问记录表
    spark.sql(
      s"""
         |drop table if exists education_online.tmp_app_access_count_${day}
       """.stripMargin)
    spark.sql(
      s"""
         |create table if not exists education_online.tmp_app_access_count_${day}
         |as
         |select
         |	uid,
         |	count(1) as access_count,
         |	dt
         |from education_online.user_behavior
         |where dt=${day}
         |group by uid,dt
       """.stripMargin)

    //从用户访问记录表查询导入到结果表
    spark.sql(
      s"""
         |insert overwrite table education_online.tmp_app_access_count_ranger_analysis_${day}
         |select
         |	count(1) as le_two,
         |	0 as le_four,
         |	0 as gt_four,
         |	dt
         |from education_online.tmp_app_access_count_${day}
         |where access_count <=2
         |group by dt
         |union all
         |select
         |	0 as le_two,
         |	count(1) as le_four,
         |	0 as gt_four,
         |	dt
         |from education_online.tmp_app_access_count_${day}
         |where access_count <=4
         |group by dt
         |union all
         |select
         |	0 as le_two,
         |	0 as le_four,
         |	count(1) as gt_four,
         |	dt
         |from education_online.tmp_app_access_count_${day}
         |where access_count >4
         |group by dt
       """.stripMargin)

  }

  /**
    * 渠道新用户统计
    *
    * @param day
    * @param spark
    * @return
    */
  def appChannelAnalysis(day: String, spark: SparkSession) = {
    spark.sql(
      s"""
         |create table if not exists education_online.tmp_app_channel_analysis_${day}(
         |	channel string,
         |	new_user_count int,
         |	dt int
         |)row format delimited fields terminated by '\t'
         |location '/warehouse/education_online/tmp/tmp_app_channel_analysis_${day}'
       """.stripMargin)

    spark.sql(
      s"""
         |insert overwrite table education_online.tmp_app_channel_analysis_${day}
         |select
         |	channel,
         |	count(distinct uid),
         |	dt
         |from education_online.user_behavior
         |where dt=${day}
         |and event_key="registerAccount"
         |group by channel,dt
       """.stripMargin)
  }

  /**
    * 各系统版本访问统计
    *
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
    *
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

  /**
    * 测试
    */
  def appTest(spark: SparkSession) = {
    //    spark.sql(
    //      s"""
    //         |create external table gmall.dws_user_action1
    //         |(
    //         |    user_id          string      comment '用户 id',
    //         |    order_count     bigint      comment '下单次数 ',
    //         |    order_amount    decimal(16,2)  comment '下单金额 ',
    //         |    payment_count   bigint      comment '支付次数',
    //         |    payment_amount  decimal(16,2) comment '支付金额 '
    //         |) COMMENT '每日用户行为宽表'
    //         |PARTITIONED BY (`dt` string)
    //         |stored as parquet
    //         |location '/warehouse/gmall/dws/dws_user_action1/'
    //         |tblproperties ("parquet.compression"="snappy")
    //       """.stripMargin)
    spark.sql(
      s"""
         |insert overwrite table gmall.dws_user_action1 partition(dt=20191205)
         |select * from gmall.dws_user_action where dt=20191205
         """.stripMargin)
  }
}
