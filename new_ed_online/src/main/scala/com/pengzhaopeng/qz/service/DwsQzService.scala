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
    dwdQzChapterList.createOrReplaceTempView("qz_chapter_list")
    dwdQzPoint.createOrReplaceTempView("dwd_qz_point")
    dwdQzPointQuestion.createOrReplaceTempView("dwd_qz_point_question")

    val dwsQzChapterDf = sparkSession.sql(
      s"""
         |select
         |	chapterid,
         |	chapterlistid,
         |	chaptername,
         |	sequence,  showstatus,  showstatus,
         |	chapter_creator,  chapter_createtime,  chapter_courseid,  chapternum,
         |	chapterallnum,  outchapterid,  chapterlistname,
         |	pointid,  questionid,  questype,  pointname,  pointyear,  chapter,
         |	excisenum,  pointlistid,  pointdescribe,
         |	pointlevel,  typelist,  point_score,  thought,  remid,  pointnamelist,
         |	typelistids,  pointlist,  dt,  dn
         |from
         |dwd_qz_chapter t1
         |join qz_chapter_list t2 on t1.chapterlistid = t2.chapterlistid and t1.dn = t2.dn
         |join dwd_qz_point t3 on t1.chapterid = t3.chapterid and t1.dn = t3.dn
         |join dwd_qz_point_question t4 on t3.pointid = t4.pointid and t3.dn = t4.dn
       """.stripMargin)
    dwsQzChapterDf.show()
    dwsQzChapterDf.write.mode(SaveMode.Append).insertInto("dws.dws_qz_chapter")
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
//  def saveDwsQzCourse(sparkSession:SparkSession,dt:String) = {
//    val dwdQzSiteCourse = QzCourseDao.getDwdQzSiteCourse(sparkSession, dt)
//    val dwdQzCourse = QzCourseDao.getDwdQzCourse(sparkSession, dt)
//    val dwdQzCourseEdusubject = QzCourseDao.getDwdQzCourseEduSubject(sparkSession, dt)
//
//    dwdQzSiteCourse.createOrReplaceTempView()
//  }
}
