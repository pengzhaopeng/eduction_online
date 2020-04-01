package com.pengzhaopeng.qz.service

import com.pengzhaopeng.qz.dao._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * @author 17688700269
  * @date 2020/3/31 17:11
  * @Version 1.0
  * @description 维度退化合成宽表
  */
object DwsQzService {

  /**
    * 章节维度退化宽表
    * 1、章节数据表               dwd.dwd_qz_chapter
    * 2、章节列表数据表           dwd.qz_chapter_list
    * 3、知识点数据日志表         dwd.dwd_qz_point
    * 4、做题知识点关联数据表     dwd.dwd_qz_point_question
    *
    * @param sparkSession
    * @param dt
    */
  def saveDwsQzChapter(sparkSession: SparkSession, dt: String) = {
    val dwdQzChapter: DataFrame = QzChapterDao.getDwdQzChapter(sparkSession, dt)
    val dwdQzChapterList: DataFrame = QzChapterDao.getDwdQzChapterList(sparkSession, dt)
    val dwdQzPoint: DataFrame = QzChapterDao.getDwdQzPoint(sparkSession, dt)
    val dwdQzPointQuestion: DataFrame = QzChapterDao.getDwdQzPointQuestion(sparkSession, dt)
    //注册临时表
    dwdQzChapter.createOrReplaceTempView("dwd_qz_chapter")
    dwdQzChapterList.createOrReplaceTempView("dwd_qz_chapter_list")
    dwdQzPoint.createOrReplaceTempView("dwd_qz_point")
    dwdQzPointQuestion.createOrReplaceTempView("dwd_qz_point_question")

    val dwsQzChapterDf = sparkSession.sql(
      s"""
         |select
         |	t1.chapterid,
         |	t1.chapterlistid,
         |	chaptername,
         |	sequence,
         | showstatus,  showstatus,
         |	chapter_creator,  chapter_createtime,  chapter_courseid,  chapternum,
         |	chapterallnum,  outchapterid,  chapterlistname,
         |	t3.pointid,  questionid,  questype,  pointname,  pointyear,  chapter,
         |	excisenum,  pointlistid,  pointdescribe,
         |	pointlevel,  typelist,  point_score,  thought,  remid,  pointnamelist,
         |	typelistids,  pointlist,
         | dt,
         | t1.dn
         |from
         |dwd_qz_chapter t1
         |join dwd_qz_chapter_list t2 on t1.chapterlistid = t2.chapterlistid and t1.dn = t2.dn
         |join dwd_qz_point t3 on t1.chapterid = t3.chapterid and t1.dn = t3.dn
         |join dwd_qz_point_question t4 on t3.pointid = t4.pointid and t3.dn = t4.dn
       """.stripMargin)
    //    dwsQzChapterDf.show()
    dwsQzChapterDf.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_chapter")
  }

  /**
    * 课程维度退化宽表 （课程--辅导id--题--知识点）
    * dwd.dwd_qz_site_course		          网站课程日志数据
    * dwd.qz_course 				              题库课程数据
    * dwd.dwd_qz_course_edusubject			  课程辅导数据
    *
    * @param sparkSession
    * @param dt
    */
  def saveDwsQzCourse(sparkSession: SparkSession, dt: String) = {
    val dwdQzSiteCourse = QzCourseDao.getDwdQzSiteCourse(sparkSession, dt)
    dwdQzSiteCourse.cache().count()
    val dwdQzCourse = QzCourseDao.getDwdQzCourse(sparkSession, dt)
    dwdQzCourse.cache().count()
    val dwdQzCourseEdusubject = QzCourseDao.getDwdQzCourseEduSubject(sparkSession, dt)
    dwdQzCourseEdusubject.cache().count()

    dwdQzSiteCourse.createOrReplaceTempView("dwd_qz_site_course")
    dwdQzCourse.createOrReplaceTempView("qz_course")
    dwdQzCourseEdusubject.createOrReplaceTempView("dwd_qz_course_edusubject")

    val dwsQzCourseDf = sparkSession.sql(
      s"""
         |select
         |	t1.sitecourseid,
         |	t1.siteid,
         |	t1.courseid,
         |	t1.sitecoursename,
         |	t1.coursechapter,
         |	t1.sequence,
         |	t1.status,
         |	t1.sitecourse_creator,
         |	t1.sitecourse_createtime,
         |	t1.helppaperstatus,
         |	t1.servertype,
         |	t1.boardid,
         |	t1.showstatus,
         |	t2.majorid,
         |	t2.coursename,
         |	t2.isadvc,
         |	t2.chapterlistid,
         |	t2.pointlistid,
         |	t3.courseeduid,
         |	t3.edusubjectid,
         |	t1.dt,
         |	t1.dn
         |from dwd_qz_site_course t1
         |join qz_course t2 on t1.courseid=t2.courseid and t1.dn=t2.dn
         |join dwd_qz_course_edusubject t3 on t1.courseid=t3.courseid and t1.dn=t3.dn
       """.stripMargin)
    dwsQzCourseDf.show()
    //    dwsQzCourseDf.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_course")
  }

  /**
    * 主修维度宽表
    * dwd.dwd_qz_major 			主修数据
    * dwd.dwd_qz_website			做题网站日志数据
    * dwd_qz_business				所属行业数据
    *
    * @param sparkSession
    * @param dt
    */
  def saveDwsQzMajor(sparkSession: SparkSession, dt: String) = {
    val dwdQzMajor = QzMajorDao.getQzMajor(sparkSession, dt)
    val dwdQzWebsite = QzMajorDao.getQzWebsite(sparkSession, dt)
    val dwdQzBusiness = QzMajorDao.getQzBusiness(sparkSession, dt)
    dwdQzMajor.createOrReplaceTempView("dwd_qz_major")
    dwdQzWebsite.createOrReplaceTempView("dwd_qz_website")
    dwdQzBusiness.createOrReplaceTempView("dwd_qz_business")

    val dwsQzMajorDf = sparkSession.sql(
      s"""
         |select
         |	 t1.majorid,
         |	 t1.businessid,
         |	 t1.siteid,
         |	 t1.majorname,
         |	 t1.shortname,
         |	 t1.status,
         |	 t1.sequence,
         |	 t1.major_creator,
         |	 t1.major_createtime,
         |	 t3.businessname,
         |	 t2.sitename,
         |	 t2.domain,
         |	 t2.multicastserver,
         |	 t2.templateserver,
         |	 t2.multicastgateway,
         |	 t2.multicastport,
         |	 t1.dt,
         |	 t1.dn
         |from dwd_qz_major t1
         |join dwd_qz_website t2 on t1.siteid=t2.siteid and t1.dn=t2.dn
         |join dwd_qz_business t3 on  t1.businessid=t3.businessid and t1.dn=t3.dn
       """.stripMargin)
    dwsQzMajorDf.show()
    //    dwsQzMajorDf.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_major")
  }

  /**
    * qz_paper_view				试卷视图数据
    * qz_center_paper     试卷主题关联数据
    * qz_center					  主题数据
    * qz_paper					  做题试卷日志数据
    */
  def saveDwsQzPager(sparkSession: SparkSession, dt: String): Unit = {
    val dwdQzPaperView = QzPaperDao.getDwdQzPaperView(sparkSession, dt)
    val dwdQzCenterPaper = QzPaperDao.getDwdQzCenterPaper(sparkSession, dt)
    val dwdQzCenter = QzPaperDao.getDwdQzCenter(sparkSession, dt)
    val dwdQzPaper = QzPaperDao.getDwdQzPaper(sparkSession, dt)
    dwdQzPaperView.createOrReplaceTempView("qz_paper_view")
    dwdQzCenterPaper.createOrReplaceTempView("qz_center_paper")
    dwdQzCenter.createOrReplaceTempView("qz_center")
    dwdQzPaper.createOrReplaceTempView("qz_paper")

    val dwsQzPagerDf = sparkSession.sql(
      s"""
         |select
         |	 t1.paperviewid,
         |	 t1.paperid,
         |	 t1.paperviewname,
         |	 t1.paperparam,
         |	 t1.openstatus,
         |	 t1.explainurl,
         |	 t1.iscontest,
         |	 t1.contesttime,
         |	 t1.conteststarttime,
         |	 t1.contestendtime,
         |	 t1.contesttimelimit,
         |	 t1.dayiid,
         |	 t1.status,
         |	 t1.paper_view_creator,
         |	 t1.paper_view_createtime,
         |	 t1.paperviewcatid,
         |	 t1.modifystatus,
         |	 t1.description,
         |	 t1.paperuse,
         |	 t1.paperdifficult,
         |	 t1.testreport,
         |	 t1.paperuseshow,
         |	 t2.centerid,
         |	 t2.sequence,
         |	 t3.centername,
         |	 t3.centeryear,
         |	 t3.centertype,
         |	 t3.provideuser,
         |	 t3.centerviewtype,
         |	 t3.stage,
         |	 t4.papercatid,
         |	 t4.courseid,
         |	 t4.paperyear,
         |	 t4.suitnum,
         |	 t4.papername,
         |	 t4.totalscore,
         |	 t4.chapterid,
         |	 t4.chapterlistid,
         |	 t1.dt,
         |	 t1.dn
         |from qz_paper_view t1
         |left join qz_center_paper t2 on t1.paperviewid=t2.paperviewid and t1.dn=t2.dn
         |left join qz_center t3 on t2.centerid=t3.centerid and t2.dn=t3.dn
         |join qz_paper t4 on t1.paperid=t4.paperid and t1.dn=t2.dn
       """.stripMargin)

    dwsQzPagerDf.show()
//    dwsQzPagerDf.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_paper")
  }

  /**
    * qz_question					  做题日志数据
    * qz_question_type				题目类型数据
    */
  def saveDwsQzQuestion(sparkSession: SparkSession, dt: String) = {
    val dwdQzQuestion = QzQuestionDao.getQzQuestion(sparkSession, dt)
    val dwdQzQuestionType = QzQuestionDao.getQzQuestionType(sparkSession, dt)
    dwdQzQuestion.createOrReplaceTempView("qz_question")
    dwdQzQuestionType.createOrReplaceTempView("qz_question_type")

    val dwsQzQuestionDf = sparkSession.sql(
      s"""
         |select
         |	t1.questionid,
         |	t1.parentid,
         |	t1.questypeid,
         |	t1.quesviewtype,
         |	t1.content,
         |	t1.answer,
         |	t1.analysis,
         |	t1.limitminute,
         |	t1.score,
         |	t1.splitscore,
         |	t1.status,
         |	t1.optnum,
         |	t1.lecture,
         |	t1.creator,
         |	t1.createtime,
         |	t1.modifystatus,
         |	t1.attanswer,
         |	t1.questag,
         |	t1.vanalysisaddr,
         |	t1.difficulty,
         |	t1.quesskill,
         |	t1.vdeoaddr,
         |	t1.viewtypename,
         |	t2.papertypename,
         |    t2.remark,
         |	t2.splitscoretype,
         |	t1.dt,
         |	t1.dn
         |from qz_question t1
         |join qz_question_type t2 on t1.questypeid=t2.questypeid and t1.dn=t2.dn
       """.stripMargin)
    dwsQzQuestionDf.show()
    dwsQzQuestionDf.coalesce(1).write.mode(SaveMode.Append).insertInto("coalesce")

  }
}
