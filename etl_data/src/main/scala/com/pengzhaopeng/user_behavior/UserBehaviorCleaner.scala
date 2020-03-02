package com.pengzhaopeng.user_behavior

import com.pengzhaopeng.utils.StringUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 用户行为数据清洗
  * 1、验证数据格式是否正确，切分后长度必须为17
  * 2、手机号脱敏，格式为123xxxx4567
  * 3、去掉username中带有的\n，否则导致写入HDFS时会换行
  * author 鹏鹏鹏先森
  * date 2020/3/2 0:27
  * Version 1.0
  */
object UserBehaviorCleaner {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: please input inputPath adn outputPath")
      System.exit(1)
    }

    //获取输入输出路径
    val inputPath: String = args(0)
    val outputPath: String = args(1)

    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    // 通过输入路径获取RDD
    val eventRDD: RDD[String] = sc.textFile(inputPath)

    //清晰数据
    eventRDD.filter(event => checkEventValid(event)) //验证数据有效性
      .map(event => maskPhone(event)) //手机号脱敏
      .map(event => repairUserName(event)) //修改userName中带有\n导致的换行
      .saveAsTextFile(outputPath)
    //停止
    sc.stop()
  }

  /**
    * username为用户自定义的，里面有要能存在"\n"，导致写入到HDFS时换行
    *
    * @param event
    */
  def repairUserName(event: String) = {
    val fields: Array[String] = event.split("\t")
    val userName: String = fields(1)

    // 用户昵称不为空时替换"\n"
    if (StringUtil.isNotEmpty(userName)) {
      fields(1) = userName.replace("\n", "")
    }
    fields.mkString("\t")
  }

  /**
    * 手机号脱敏
    *
    * @param event
    */
  def maskPhone(event: String) = {
    var maskPhone = new StringBuilder
    val fields: Array[String] = event.split("\t")
    val phone: String = fields(9)

    if (StringUtil.isNotEmpty(phone)) {
      maskPhone.append(phone.substring(0, 3))
        .append("xxxx")
        .append(phone.substring(7, 11))
    }
    fields.mkString("\t")
  }

  /**
    * 验证数据格式是否正确，只有切分后长度为17的才算正确
    *
    * @param event
    */
  def checkEventValid(event: String) = {
    val fields: Array[String] = event.split("\t")
    fields.length == 17
  }
}
