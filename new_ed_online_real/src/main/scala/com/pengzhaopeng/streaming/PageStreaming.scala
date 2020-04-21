package com.pengzhaopeng.streaming

import java.lang
import java.sql.{Connection, ResultSet}

import com.alibaba.fastjson.JSONObject
import com.pengzhaopeng.bean.Page
import com.pengzhaopeng.streaming.QzPointStreaming.batchDuration
import com.pengzhaopeng.utils._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._

import scala.collection.mutable

/**
  * @author 17688700269
  * @date 2020/4/20 21:52
  * @Version 1.0
  * @description 页面转换率实时统计
  */
object PageStreaming {

  private val batchDuration = 3

  def main(args: Array[String]): Unit = {
    //参数设置
    System.setProperty("HADOOP_USER_NAME", "dog")
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(batchDuration))
    //kafka参数
    val topics = Array(ConfigurationManager.getProperty("kafka.topic.qc.page"))
    val groupid = ConfigurationManager.getProperty("kafka.topic.qc.page.groupid")
    val kafkaMap: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> ConfigurationManager.getProperty("kafka.bootstrap.servers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    //查询mysql中是否存在偏移量
    val client = DataSourceUtil.getConnection
    val sqlProxy = new SqlProxy
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
    try {
      sqlProxy.executeQuery(client, "select * from `offset_manager` where groupid=?", Array(groupid), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            val model = new TopicPartition(rs.getString(2), rs.getInt(3))
            val offset = rs.getLong(4)
            offsetMap.put(model, offset)
          }
          rs.close()
        }
      })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      sqlProxy.shutdown(client)
    }

    //设置kafka的消费参数 有偏移量就继续消费 没有就重新消费
    val stream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty) {
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap)
      )
    } else {
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap, offsetMap)
      )
    }

    //过滤清洗数据
    val filterDs: DStream[Page] = stream.map(item => item.value())
      .mapPartitions(partition => {
        partition.map(item => {
          val jsonObject: JSONObject = ParseJsonData.getJsonData(item)
          val uid = if (jsonObject.containsKey("uid")) jsonObject.getString("uid") else ""
          val app_id = if (jsonObject.containsKey("app_id")) jsonObject.getString("app_id") else ""
          val device_id = if (jsonObject.containsKey("device_id")) jsonObject.getString("device_id") else ""
          val ip = if (jsonObject.containsKey("ip")) jsonObject.getString("ip") else ""
          val last_page_id = if (jsonObject.containsKey("last_page_id")) jsonObject.getString("last_page_id") else ""
          val page_id = if (jsonObject.containsKey("page_id")) jsonObject.getString("page_id") else ""
          val next_page_id = if (jsonObject.containsKey("next_page_id")) jsonObject.getString("next_page_id") else ""
          Page(uid, app_id, device_id, ip, last_page_id, page_id, next_page_id)
        })
      })
    //      .filter(item => {
    //        !item.last_page_id.equals("") && !item.page_id.equals("") && !item.next_page_id.equals("")
    //      })
    //下面业务会多次使用到 filterRDD 这里先缓存
    filterDs.cache()

    //业务
    val resultDS: DStream[(String, Int)] = filterDs.map(item => (item.last_page_id + "_" + item.page_id + "_" + item.next_page_id, 1))
      .reduceByKey(_ + _)
    resultDS.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        //在分区下获取jdbc连接
        val sqlProxy = new SqlProxy
        val client = DataSourceUtil.getConnection
        try {
          partition.foreach(item => {
            //计算页面跳转个数
            calcPageJumCount(sqlProxy, item, client)
          })
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(client)
        }
      })
    })


    //处理完业务逻辑 提交 offset 到mysql
    stream.foreachRDD(rdd => {
      val sqlProxy = new SqlProxy
      val client = DataSourceUtil.getConnection
      try {
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (elem <- offsetRanges) {
          sqlProxy.executeUpdate(client, "replace into `offset_manager` (`groupid`,`topic`,`partition`,untilOffset) values(?,?,?,?)",
            Array(groupid, elem.topic, elem.partition.toString, elem.untilOffset))
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        sqlProxy.shutdown(client)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 计算页面跳转个数
    *
    * @param sqlProxy
    * @param item
    * @param client
    */
  def calcPageJumCount(sqlProxy: SqlProxy, item: (String, Int), client: Connection) = {
    //从mysql取当前page 的跳转个数，跟当前批次进行累加，再更新回mysql
    val fileds: Array[String] = item._1.split("_")
    val last_page_id: Int = fileds(0).toInt //上一个page_id
    val page_id: Int = fileds(1).toInt //当前page_id
    val next_page_id = fileds(2).toInt //下一个page_id
    //查询 page_id 的历史个数
    val pageIdNumHistorySql =
    s"""
       |select
       |  num
       |from page_jump_rate
       |where page_id=?
       """.stripMargin
    var num: Long = item._2
    sqlProxy.executeQuery(client, pageIdNumHistorySql, Array(page_id), new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          num += rs.getLong(1)
        }
        rs.close()
      }
    })
    //判断page_id是否是首页，是首页当前page_id跳转率就是100%
    if (page_id == 1) {
      val updateSql: String =
        s"""
           |insert into page_jump_rate(last_page_id,page_id,next_page_id,num,jump_rate)
           |values(?,?,?,?,?)
           |on duplicate key update num=num+?
         """.stripMargin
      sqlProxy.executeUpdate(client, updateSql, Array(last_page_id, page_id, next_page_id, num, "100%", num))
    } else {
      val updateSql: String =
        s"""
           |insert into page_jump_rate(last_page_id,page_id,next_page_id,num)
           |values(?,?,?,?)
           |on duplicate key update num=num+?
         """.stripMargin
      sqlProxy.executeUpdate(client, updateSql, Array(last_page_id, page_id, next_page_id, num, num))
    }
  }
}
