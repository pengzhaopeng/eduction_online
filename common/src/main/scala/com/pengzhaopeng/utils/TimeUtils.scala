package com.pengzhaopeng.utils

import java.text.SimpleDateFormat
import java.util.Calendar

import com.github.nscala_time.time.Imports.DateTime

/**
  * @author 鹏鹏鹏先森
  * @date 2020/2/24 21:11
  * @Version 1.0
  * @description
  */
object TimeUtils {

  def main(args: Array[String]): Unit = {
    //        val time: Long = convertDateStr2TimeStamp("2020-02-24", DAY_DATE_FORMAT_ONE)
    //    val time: Int = dayOfWeek("2020-02-25")
    //    val time = dateFormat("2020-02-24", "yyyy年MM月dd日")
    import com.github.nscala_time.time.Imports._
    val time = DateTime.parse("20150101", DateTimeFormat.forPattern("yyyyMMdd"))
    println("结果时间：" + DateTime.formatted(SECOND_DATE_FORMAT))
  }

  final val ONE_HOUR_MILLISECONDS = 60 * 60 * 1000

  final val SECOND_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"

  final val DAY_DATE_FORMAT_ONE = "yyyy-MM-dd"

  final val DAY_DATE_FORMAT_TWO = "yyyyMMdd"

  //  def getCurrentTime(): Unit ={
  //
  //  }

  //时间字符串=>时间戳
  def convertDateStr2TimeStamp(dateStr: String, pattern: String): Long = {
    new SimpleDateFormat(pattern).parse(dateStr).getTime
  }

  //时间字符串+天数=>时间戳
  //  def dateStrAddDays2TimeStamp(dateStr: String, pattern: String, days: Int): Long = {
  //    convertDateStr2Date(dateStr, pattern).plusDays(days).date.getTime
  //  }


  //时间字符串=>日期
  def convertDateStr2Date(dateStr: String, pattern: String): DateTime = {
    new DateTime(new SimpleDateFormat(pattern).parse(dateStr))
  }


  //时间戳=>日期
  def convertTimeStamp2Date(timestamp: Long): DateTime = {
    new DateTime(timestamp)
  }

  //时间戳=>字符串
  def convertTimeStamp2DateStr(timestamp: Long, pattern: String): String = {
    new DateTime(timestamp).toString(pattern)
  }

  //时间戳=>小时数
  def convertTimeStamp2Hour(timestamp: Long): Long = {
    new DateTime(timestamp).hourOfDay().getAsString().toLong
  }


  //时间戳=>分钟数
  def convertTimeStamp2Minute(timestamp: Long): Long = {
    new DateTime(timestamp).minuteOfHour().getAsString().toLong
  }

  //时间戳=>秒数
  def convertTimeStamp2Sec(timestamp: Long): Long = {
    new DateTime(timestamp).secondOfMinute().getAsString.toLong
  }


  def addZero(hourOrMin: String): String = {
    if (hourOrMin.toInt <= 9)
      "0" + hourOrMin
    else
      hourOrMin

  }

  def delZero(hourOrMin: String): String = {
    var res = hourOrMin
    if (!hourOrMin.equals("0") && hourOrMin.startsWith("0"))
      res = res.replaceAll("^0", "")
    res
  }

  def dateStrPatternOne2Two(time: String): String = {
    TimeUtils.convertTimeStamp2DateStr(TimeUtils.convertDateStr2TimeStamp(time, TimeUtils
      .DAY_DATE_FORMAT_ONE), TimeUtils.DAY_DATE_FORMAT_TWO)
  }

  //获取星期几
  def dayOfWeek(dateStr: String): Int = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val date = sdf.parse(dateStr)

    //    val sdf2 = new SimpleDateFormat("EEEE")
    //    sdf2.format(date)

    val cal = Calendar.getInstance()
    cal.setTime(date);
    var w = cal.get(Calendar.DAY_OF_WEEK) - 1

    //星期天 默认为0
    if (w <= 0)
      w = 7
    w
  }

  //判断是否是周末
  def isRestday(date: String): Boolean = {
    val dayNumOfWeek = dayOfWeek(date)
    dayNumOfWeek == 6 || dayNumOfWeek == 7
  }

}
