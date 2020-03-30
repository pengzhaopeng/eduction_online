package com.pengzhaopeng.member.service

import com.pengzhaopeng.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

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
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
    HiveUtil.openCompression(sparkSession) //开启压缩
    HiveUtil.useSnappyCompression(sparkSession) //使用snappy压缩

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
         |where a.dt=20190722
         |limit 10
       """.stripMargin).show()
  }
}
