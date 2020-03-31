package com.pengzhaopeng.member.service

import com.pengzhaopeng.member.bean.DwsMember
import com.pengzhaopeng.member.dao.DwdMemberDao
import com.pengzhaopeng.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
  * @author 17688700269
  * @date 2020/3/30 15:45
  * @Version 1.0
  * @description
  */
object DwsMemberService {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "dog")
    val sparkConf = new SparkConf().setAppName("dws_member_import")
      .setMaster("local[*]")
      //      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //      .registerKryoClasses(Array(classOf[DwsMember]))
      .set("spark.sql.autoBroadcastJoinThreshold", "104857600") //100m
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
    HiveUtil.openCompression(sparkSession) //开启压缩
    HiveUtil.useSnappyCompression(sparkSession) //使用snappy压缩

//        importMemberUseApi(sparkSession, "20190722")
    importMemberUseSql(sparkSession, "20190722")
    //    importMemberUseSqlByBroadcast(sparkSession, "20190722")
  }

  /**
    * 使用sql方式 + 默认走 sortMerageJoin
    *
    * @param sparkSession
    * @param dt
    */
  def importMemberUseSql(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(
      s"""
         |select
         |	a.uid,a.ad_id,a.fullname,a.iconurl,a.lastlogin,a.mailaddr,a.memberlevel,
         |	a.password,e.paymoney,a.phone,a.qq,a.register,a.regupdatetime,a.unitname,a.userip,
         |	a.zipcode,a.dt,b.appkey,b.appregurl,b.bdp_uuid,b.createtime as reg_createtime,
         |	b.isranreg,b.regsource,
         |	b.regsourcename,c.adname,d.siteid,d.sitename,d.siteurl,d.delete as site_delete,d.createtime as site_createtime,
         |	d.creator as site_creator,f.vip_id,f.vip_level,f.start_time as vip_start_time,f.end_time as vip_end_time,
         |	f.last_modify_time as vip_last_modify_time,f.max_free as vip_max_free,f.min_free as vip_min_free,
         |	f.next_level as vip_next_level,f.operator as vip_operator,a.dn
         |from dwd.dwd_member a
         |left join dwd.dwd_member_regtype b on a.uid=b.uid and a.dn=b.dn
         |left join dwd.dwd_base_ad c on a.ad_id=c.adid and a.dn=c.dn
         |left join dwd.dwd_base_website d on b.websiteid=d.siteid and b.dn=d.dn
         |left join dwd.dwd_pcentermempaymoney e on a.uid=e.uid and a.dn=e.dn
         |left join dwd.dwd_vip_level f on e.vip_id=f.vip_id and e.dn=f.dn
         |where a.dt=${dt}
           """.stripMargin).show()
    while (true) {
      Thread.sleep(5000)
      println("sql")
    }
  }

  /**
    * 使用sql方式 + 广播小表 （spark2.1中式不生效的，没有对纯sql进行优化）
    *
    * @param sparkSession
    * @param dt
    */
  def importMemberUseSqlByBroadcast(sparkSession: SparkSession, dt: String) = {
    val dwdMember = DwdMemberDao.getDwdMember(sparkSession) //主表用户表
    val dwdMemberRegtype = DwdMemberDao.getDwdMemberRegType(sparkSession)
    val dwdBaseAd = DwdMemberDao.getDwdBaseAd(sparkSession)
    val dwdBaseWebsite = DwdMemberDao.getDwdBaseWebSite(sparkSession)
    val dwdPcentermemPaymoney = DwdMemberDao.getDwdPcentermemPayMoney(sparkSession)
    val dwdVipLevel = DwdMemberDao.getDwdVipLevel(sparkSession)

    //大表
    dwdMember.registerTempTable("dwd_member")
    dwdMemberRegtype.registerTempTable("dwd_member_regtype")

    //需要广播的小表
    dwdBaseAd.cache().count
    dwdBaseAd.registerTempTable("dwd_base_ad")
    dwdBaseWebsite.cache().count
    dwdBaseWebsite.registerTempTable("dwd_base_website")
    dwdPcentermemPaymoney.cache().count()
    dwdPcentermemPaymoney.registerTempTable("dwd_pcentermempaymoney")
    dwdVipLevel.cache().count()
    dwdVipLevel.registerTempTable("dwd_vip_level")

    sparkSession.sql(
      s"""
         |select
         |	a.uid,a.ad_id,a.fullname,a.iconurl,a.lastlogin,a.mailaddr,a.memberlevel,
         |	a.password,e.paymoney,a.phone,a.qq,a.register,a.regupdatetime,a.unitname,a.userip,
         |	a.zipcode,a.dt,b.appkey,b.appregurl,b.bdp_uuid,b.createtime as reg_createtime,
         |	b.isranreg,b.regsource,
         |	b.regsourcename,c.adname,d.siteid,d.sitename,d.siteurl,d.delete as site_delete,d.createtime as site_createtime,
         |	d.creator as site_creator,f.vip_id,f.vip_level,f.start_time as vip_start_time,f.end_time as vip_end_time,
         |	f.last_modify_time as vip_last_modify_time,f.max_free as vip_max_free,f.min_free as vip_min_free,
         |	f.next_level as vip_next_level,f.operator as vip_operator,a.dn
         |from dwd_member a
         |left join dwd_member_regtype b on a.uid=b.uid and a.dn=b.dn
         |left join dwd_base_ad c on a.ad_id=c.adid and a.dn=c.dn
         |left join dwd_base_website d on b.websiteid=d.siteid and b.dn=d.dn
         |left join dwd_pcentermempaymoney e on a.uid=e.uid and a.dn=e.dn
         |left join dwd_vip_level f on e.vip_id=f.vip_id and e.dn=f.dn
         |where a.dt=20190722
           """.stripMargin).show()
    while (true) {
      Thread.sleep(5000)
      println("sql")
    }
  }

  /**
    * 使用 api 方式 + 广播小表
    *
    * @param sparkSession
    * @param dt
    */
  def importMemberUseApi(sparkSession: SparkSession, dt: String) = {
    import sparkSession.implicits._
    val dwdMember = DwdMemberDao.getDwdMember(sparkSession).where(s"dt='${dt}'") //主表用户表
    val dwdMemberRegtype = DwdMemberDao.getDwdMemberRegType(sparkSession)
    val dwdBaseAd = DwdMemberDao.getDwdBaseAd(sparkSession)
    val dwdBaseWebsite = DwdMemberDao.getDwdBaseWebSite(sparkSession)
    val dwdPcentermemPaymoney = DwdMemberDao.getDwdPcentermemPayMoney(sparkSession)
    val dwdVipLevel = DwdMemberDao.getDwdVipLevel(sparkSession)
    import org.apache.spark.sql.functions.broadcast
    val result = dwdMember.join(dwdMemberRegtype, Seq("uid", "dn"), "left_outer")
      .join(broadcast(dwdBaseAd), Seq("ad_id", "dn"), "left_outer")
      .join(broadcast(dwdBaseWebsite), Seq("siteid", "dn"), "left_outer")
      .join(broadcast(dwdPcentermemPaymoney), Seq("uid", "dn"), "left_outer")
      .join(broadcast(dwdVipLevel), Seq("vip_id", "dn"), "left_outer")
      .select("uid", "ad_id", "fullname", "iconurl", "lastlogin", "mailaddr", "memberlevel", "password"
        , "paymoney", "phone", "qq", "register", "regupdatetime", "unitname", "userip", "zipcode", "appkey"
        , "appregurl", "bdp_uuid", "reg_createtime", "isranreg", "regsource", "regsourcename", "adname"
        , "siteid", "sitename", "siteurl", "site_delete", "site_createtime", "site_creator", "vip_id", "vip_level",
        "vip_start_time", "vip_end_time", "vip_last_modify_time", "vip_max_free", "vip_min_free", "vip_next_level"
        , "vip_operator", "dt", "dn")
      //      .as[DwsMember].rdd
//      .as[DwsMember]
//    result.cache() //序列化缓存
    result.show()
    while (true) {
      Thread.sleep(5000)
      println("api")
    }
  }
}
