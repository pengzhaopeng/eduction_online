package com.pengzhaopeng.bean

/**
  * @author 17688700269
  * @date 2020/3/3 16:02
  * @Version 1.0
  * @description
  * uid STRING comment "用户唯一标识",
  * username STRING comment "用户昵称",
  * gender STRING comment "性别",
  * level TINYINT comment "1代表小学，2代表初中，3代表高中",
  * is_vip TINYINT comment "0代表不是会员，1代表是会员",
  * os STRING comment "操作系统:os,android等",
  * channel STRING comment "下载渠道:auto,toutiao,huawei",
  * net_config STRING comment "当前网络类型",
  * ip STRING comment "IP地址",
  * phone STRING comment "手机号码",
  * video_id INT comment "视频id",
  * video_length INT comment "视频时长，单位秒",
  * start_video_time BIGINT comment "开始看视频的时间缀，秒级",
  * end_video_time BIGINT comment "退出视频时的时间缀，秒级",
  * version STRING comment "版本",
  * event_key STRING comment "事件类型",
  * event_time BIGINT comment "事件发生时的时间缀，秒级"
  */
case class UserBehavior(
                         uid: String,
                         username: String,
                         gender: String,
                         level: Int,
                         is_vip: Int,
                         os: String,
                         channel: String,
                         net_config: String,
                         ip: String,
                         phone: String,
                         video_id: Int,
                         video_length: Int,
                         start_video_time: Int,
                         end_video_time: Int,
                         version: String,
                         event_key: String,
                         event_time: Int
                       )

case class UserBehavior1(
                         uid: Int,
                         username: String,
                         gender: String,
                         level: String,
                         is_vip: String,
                         os: String,
                         channel: String,
                         net_config: String,
                         ip: String,
                         phone: String,
                         video_id: String,
                         video_length: String,
                         start_video_time: String,
                         end_video_time: String,
                         version: String,
                         event_key: String,
                         event_time: String
                       )

