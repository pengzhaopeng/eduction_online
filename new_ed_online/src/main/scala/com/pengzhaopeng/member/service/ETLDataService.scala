package com.pengzhaopeng.member.service

import com.alibaba.fastjson.JSONObject
import com.pengzhaopeng.util.ParseJsonData
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * @author 17688700269
  * @date 2020/3/26 17:22
  * @Version 1.0
  * @description 清洗数据
  *              1、敏感字段姓名、电话.filter不是json的数据，
  *              2、mappartion 针对每个分区去做数据循环 map 操作组装成对应表需要的字段
  *              3、重组万之后 coalesce 缩小分区个数（减少文件个数）并刷新到目标表中
  */
object ETLDataService {
  /**
    * etl用户注册信息
    * @param ssc
    * @param sparkSession
    * @return
    */
  def etlMemberRegtypeLog(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    ssc.textFile("/user/dog/ods/memberRegtype.log")
      .filter(item => {
        val obj: JSONObject = ParseJsonData.getJsonData(item)
        obj.isInstanceOf[JSONObject]
      })
      .mapPartitions(partition => {
        partition.map(item => {
          val jsonObject: JSONObject = ParseJsonData.getJsonData(item)
          val appkey = jsonObject.getString("appkey")
          val appregurl = jsonObject.getString("appregurl")
          val bdp_uuid = jsonObject.getString("bdp_uuid")
          val createtime = jsonObject.getString("createtime")
          val isranreg = jsonObject.getString("isranreg")
          val regsource = jsonObject.getString("regsource")
          val regsourceName: String = regsource match {
            case "1" => "PC"
            case "2" => "Mobile"
            case "3" => "App"
            case "4" => "WeChat"
            case _ => "other"
          }
          val uid = jsonObject.getIntValue("uid")
          val websiteid = jsonObject.getIntValue("websiteid")
          val dt = jsonObject.getString("dt")
          val dn = jsonObject.getString("dn")
          (uid, appkey, appregurl, bdp_uuid, createtime, isranreg, regsource, regsourceName, websiteid, dt, dn)
        })
      }).toDF().coalesce(1)
  }
}
