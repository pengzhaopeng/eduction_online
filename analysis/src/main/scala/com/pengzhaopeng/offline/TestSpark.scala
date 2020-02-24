package com.pengzhaopeng.offline

import com.pengzhaopeng.utils.JodaTimeUtil

/**
  * @author 鹏鹏鹏先森
  * @date 2020/2/24 22:24
  * @Version 1.0
  * @description
  */
object TestSpark {
  def main(args: Array[String]): Unit = {

    val time: Long = JodaTimeUtil.getStartOfDay()
    println(time)
  }
}
