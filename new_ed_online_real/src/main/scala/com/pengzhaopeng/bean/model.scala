package com.pengzhaopeng.bean

/**
  * description 
  * author 鹏鹏鹏先森
  * date 2020/4/13 0:43
  * Version 1.0
  * istrue 0-错误
  */
case class QzLog(
                  uid: String,
                  courserid: String,
                  pointid: String,
                  questionid: String,
                  istrue: String,
                  createTime: String
                )

case class Page(
                 uid: String,
                 app_id: String,
                 device_id: String,
                 ip: String,
                 last_page_id: String,
                 page_id: String,
                 next_page_id: String)