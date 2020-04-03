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
//    dwsQzCourseDf.show()
        dwsQzCourseDf.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_course")
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
//    dwsQzMajorDf.show()
        dwsQzMajorDf.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_major")
  }

  /**
    * 试卷维度退化宽表
    * qz_paper_view				试卷视图数据
    * qz_center_paper     试卷主题关联数据
    * qz_center					  主题数据
    * qz_paper					  做题试卷日志数据
    */
  def saveDwsQzPaper(sparkSession: SparkSession, dt: String): Unit = {
    val dwdQzPaperView = QzPaperDao.getDwdQzPaperView(sparkSession, dt)
    val dwdQzCenterPaper = QzPaperDao.getDwdQzCenterPaper(sparkSession, dt)
    val dwdQzCenter = QzPaperDao.getDwdQzCenter(sparkSession, dt)
    val dwdQzPaper = QzPaperDao.getDwdQzPaper(sparkSession, dt)
    dwdQzPaperView.createOrReplaceTempView("qz_paper_view")
    dwdQzCenterPaper.createOrReplaceTempView("qz_center_paper")
    dwdQzCenter.createOrReplaceTempView("qz_center")
    dwdQzPaper.createOrReplaceTempView("qz_paper")

    val dwsQzPaperDf = sparkSession.sql(
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

//    dwsQzPaperDf.show()
    dwsQzPaperDf.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_paper")
  }

  /**
    * 题目维度退化款表
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
         |	t2.viewtypename,
         |	t2.papertypename,
         |  t2.remark,
         |	t2.splitscoretype,
         |	t1.dt,
         |	t1.dn
         |from qz_question t1
         |join qz_question_type t2 on t1.questypeid=t2.questypeid and t1.dn=t2.dn
       """.stripMargin)
//    dwsQzQuestionDf.show()
    dwsQzQuestionDf.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_question")

  }

  /**
    * 用户做题详情退化大宽表s
    * 学员做题详情数据表 + 章节维度 + 课程维度 + 主修维度 + 试卷维度 + 题目维度
    * dws_user_paper_detail
    * dws_qz_chapter
    * dws_qz_course
    * dws_qz_major
    * dws_qz_paper
    * dws_qz_question
    */
  def saveDwsUserPaperDetail(sparkSession: SparkSession, dt: String) = {
//    val dwdQzMemberPaperQuestion = UserPaperDetailDao.getDwdQzMemberPaperQuestion(sparkSession, dt)
//    val dwsQzChapter = UserPaperDetailDao.getDwsQzChapter(sparkSession, dt)
//    val dwsQzCourse = UserPaperDetailDao.getDwsQzCourse(sparkSession, dt)
//    val dwsQzMajor = UserPaperDetailDao.getDwsQzMajor(sparkSession, dt)
//    val dwsQzPaper = UserPaperDetailDao.getDwsQzPaper(sparkSession, dt)
//    val dwsQzQuestion = UserPaperDetailDao.getDwsQzQuestion(sparkSession, dt)
//    dwdQzMemberPaperQuestion.createOrReplaceTempView("dws_user_paper_detail")
//    dwsQzChapter.createOrReplaceTempView("dws_qz_chapter")
//    dwsQzCourse.createOrReplaceTempView("dws_qz_course")
//    dwsQzMajor.createOrReplaceTempView("dws_qz_major")
//    dwsQzPaper.createOrReplaceTempView("dws_qz_paper")
//    dwsQzQuestion.createOrReplaceTempView("dws_qz_question")
//
//    val dwsUserPaperDetailDf =  sparkSession.sql(
//      s"""
//         |select
//         |	userid,
//         |	t3.courseid,
//         |	t1.questionid,
//         |	t1.useranswer,
//         |	t1.istrue,
//         |	t1.lasttime,
//         |	t1.opertype,
//         |	t1.paperid,
//         |	t1.spendtime,
//         |	t1.chapterid,
//         |	chaptername,
//         |	chapternum,
//         |  chapterallnum,
//         |	outchapterid,
//         |	chapterlistname,
//         |	pointid,
//         |	questype,
//         |	pointyear,
//         |	chapter,
//         |	pointname,
//         |	excisenum,
//         |	pointdescribe,
//         |	pointlevel,
//         |	typelist,
//         |	point_score,
//         |	thought,
//         |	remid,
//         |	pointnamelist,
//         |    typelistids,
//         |	pointlist,
//         |	t1.sitecourseid,
//         |	siteid,
//         |	sitecoursename,
//         |	coursechapter,
//         |	course_sequence,
//         |	t3.course_status,
//         |	sitecourse_creator,
//         |	t3.sitecourse_createtime,
//         |	servertype,
//         |	helppaperstatus,
//         |	boardid,
//         |	showstatus,
//         |	t1.majorid,
//         |	coursename,
//         |    isadvc,
//         |	t3.chapterlistid,
//         |	t3.pointlistid,
//         |	courseeduid,
//         |	edusubjectid,
//         |	businessid,
//         |	majorname,
//         |	shortname,
//         |    major_status,
//         |	major_sequence,
//         |	major_creator,
//         |	major_createtime,
//         |	businessname,
//         |	sitename,
//         |    domain,
//         |	multicastserver,
//         |	templateserver,
//         |	multicastgateway,
//         |	multicastport,
//         |	t1.paperviewid,
//         |	paperviewname,
//         |	paperparam,
//         |    openstatus,
//         |	explainurl,
//         |	iscontest,
//         |	contesttime,
//         |	conteststarttime,
//         |	contestendtime,
//         |	contesttimelimit,
//         |    dayiid,
//         |	paper_status,
//         |	paper_view_creator,
//         |	paper_view_createtime,
//         |	paperviewcatid,
//         |	modifystatus,
//         |	description,
//         |	paperuse,
//         |    testreport,
//         |	centerid,
//         |	paper_sequence,
//         |	centername,
//         |	centeryear,
//         |	centertype,
//         |	provideuser,
//         |	centerviewtype,
//         |    paper_stage,
//         |	papercatid,
//         |	paperyear,
//         |	suitnum,
//         |	papername,
//         |	totalscore,
//         |	question_parentid,
//         |	questypeid,
//         |    quesviewtype,
//         |	question_content,
//         |	t1.question_answer,
//         |	question_analysis,
//         |	question_limitminute,
//         |	t1.score,
//         |    splitscore,
//         |	lecture,
//         |	question_creator,
//         |	question_createtime,
//         |	question_modifystatus,
//         |	question_attanswer,
//         |    question_questag,
//         |	question_vanalysisaddr,
//         |	question_difficulty,
//         |	quesskill,
//         |	vdeoaddr,
//         |	question_description,
//         |    question_splitscoretype,
//         |	t1.question_answer,
//         |	t1.dt,
//         |	t1.dn
//         |from dws_user_paper_detail t1
//         |join dws_qz_chapter t2 on t1.chapterid=t2.chapterid and t1.dn=t2.dn
//         |join dws_qz_course t3 on t1.sitecourseid=t3.sitecourseid and t1.dn=t3.dn
//         |join dws_qz_major t4 on t1.majorid=t4.majorid and t1.dn=t4.dn
//         |join dws_qz_paper t5 on t1.paperviewid=t5.paperviewid and t1.dn=t5.dn
//         |join dws_qz_question t6 on t1.questionid=t6.questionid and t1.dn=t6.dn
//       """.stripMargin)
//
////    dwsUserPaperDetailDf.show()
//    dwsUserPaperDetailDf.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_user_paper_detail")


    val dwdQzMemberPaperQuestion = UserPaperDetailDao.getDwdQzMemberPaperQuestion(sparkSession, dt).drop("paperid")
      .withColumnRenamed("question_answer", "user_question_answer")
    val dwsQzChapter = UserPaperDetailDao.getDwsQzChapter(sparkSession, dt).drop("courseid")
    val dwsQzCourse = UserPaperDetailDao.getDwsQzCourse(sparkSession, dt).withColumnRenamed("sitecourse_creator", "course_creator")
      .withColumnRenamed("sitecourse_createtime", "course_createtime").drop("majorid")
      .drop("chapterlistid").drop("pointlistid")
    val dwsQzMajor = UserPaperDetailDao.getDwsQzMajor(sparkSession, dt)
    val dwsQzPaper = UserPaperDetailDao.getDwsQzPaper(sparkSession, dt).drop("courseid")
    val dwsQzQuestion = UserPaperDetailDao.getDwsQzQuestion(sparkSession, dt)
    dwdQzMemberPaperQuestion.join(dwsQzCourse, Seq("sitecourseid", "dn")).
      join(dwsQzChapter, Seq("chapterid", "dn")).join(dwsQzMajor, Seq("majorid", "dn"))
      .join(dwsQzPaper, Seq("paperviewid", "dn")).join(dwsQzQuestion, Seq("questionid", "dn"))
      .select("userid", "courseid", "questionid", "useranswer", "istrue", "lasttime", "opertype",
        "paperid", "spendtime", "chapterid", "chaptername", "chapternum",
        "chapterallnum", "outchapterid", "chapterlistname", "pointid", "questype", "pointyear", "chapter", "pointname"
        , "excisenum", "pointdescribe", "pointlevel", "typelist", "point_score", "thought", "remid", "pointnamelist",
        "typelistids", "pointlist", "sitecourseid", "siteid", "sitecoursename", "coursechapter", "course_sequence", "course_status"
        , "course_creator", "course_createtime", "servertype", "helppaperstatus", "boardid", "showstatus", "majorid", "coursename",
        "isadvc", "chapterlistid", "pointlistid", "courseeduid", "edusubjectid", "businessid", "majorname", "shortname",
        "major_status", "major_sequence", "major_creator", "major_createtime", "businessname", "sitename",
        "domain", "multicastserver", "templateserver", "multicastgateway", "multicastport", "paperviewid", "paperviewname", "paperparam",
        "openstatus", "explainurl", "iscontest", "contesttime", "conteststarttime", "contestendtime", "contesttimelimit",
        "dayiid", "paper_status", "paper_view_creator", "paper_view_createtime", "paperviewcatid", "modifystatus", "description", "paperuse",
        "testreport", "centerid", "paper_sequence", "centername", "centeryear", "centertype", "provideuser", "centerviewtype",
        "paper_stage", "papercatid", "paperyear", "suitnum", "papername", "totalscore", "question_parentid", "questypeid",
        "quesviewtype", "question_content", "question_answer", "question_analysis", "question_limitminute", "score",
        "splitscore", "lecture", "question_creator", "question_createtime", "question_modifystatus", "question_attanswer",
        "question_questag", "question_vanalysisaddr", "question_difficulty", "quesskill", "vdeoaddr", "question_description",
        "question_splitscoretype", "user_question_answer", "dt", "dn").coalesce(1)
      .write.mode(SaveMode.Append).insertInto("dws.dws_user_paper_detail")
  }
}
