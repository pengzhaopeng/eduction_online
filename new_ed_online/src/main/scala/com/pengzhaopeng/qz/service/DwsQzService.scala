package com.pengzhaopeng.qz.service

import com.pengzhaopeng.qz.dao.{QzChapterDao, QzCourseDao}
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
    * @param sparkSession
    * @param dt
    */
  def saveDwsQzChapter(sparkSession:SparkSession,dt:String) = {
    val dwdQzChapter: DataFrame = QzChapterDao.getDwdQzChapter(sparkSession,dt)
    val dwdQzChapterList: DataFrame = QzChapterDao.getDwdQzChapterList(sparkSession,dt)
    val dwdQzPoint: DataFrame = QzChapterDao.getDwdQzPoint(sparkSession,dt)
    val dwdQzPointQuestion: DataFrame = QzChapterDao.getDwdQzPointQuestion(sparkSession,dt)
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
    dwsQzChapterDf.show()
//    dwsQzChapterDf.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_chapter")
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
  def saveDwsQzCourse(sparkSession:SparkSession,dt:String) = {
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
//    dwsQzCourseDf.show()
    dwsQzCourseDf.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_course")
  }
}
