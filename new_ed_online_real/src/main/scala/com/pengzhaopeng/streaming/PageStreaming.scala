package com.pengzhaopeng.streaming

import java.lang
import java.sql.{Connection, ResultSet}
import java.text.NumberFormat

import com.alibaba.fastjson.JSONObject
import com.pengzhaopeng.bean.Page
import com.pengzhaopeng.streaming.QzPointStreaming.batchDuration
import com.pengzhaopeng.utils.{SqlProxy, _}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkFiles}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.lionsoul.ip2region.{DbConfig, DbSearcher}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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
          clearData(item) //清洗数据
        })
      })
    //      .filter(item => {
    //        !item.last_page_id.equals("") && !item.page_id.equals("") && !item.next_page_id.equals("")
    //      })
    //下面业务会多次使用到 filterRDD 这里先缓存
    filterDs.cache()

    //业务
    //需求1：计算各个页面的跳转个数
    val resultDS: DStream[(String, Int)] = filterDs.map(item => (item.last_page_id + "_" + item.page_id + "_" + item.next_page_id, 1))
      .reduceByKey(_ + _)
    resultDS.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        //在分区下获取jdbc连接
        val sqlProxy = new SqlProxy
        val client: Connection = DataSourceUtil.getConnection
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

    //需求3：根据ip得出相应省份，展示出 top3 省份的点击数，需要根据历史数据累加
    ipForTop3Province(ssc, filterDs)

    //处理完业务逻辑 提交 offset 到mysql
    stream.foreachRDD(rdd => {
      val sqlProxy = new SqlProxy
      val client = DataSourceUtil.getConnection
      //需求2：根据页面跳转个数表计算转换率，没必要放到sparkStreaming中来做,只要处理一次，所以这里放driver端来做,处理完直接更新偏移量
      calcJumRate(sqlProxy, client)
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
    * 清洗数据
    *
    * @param item
    * @return
    */
  private def clearData(item: String) = {
    val jsonObject: JSONObject = ParseJsonData.getJsonData(item)
    val uid = if (jsonObject.containsKey("uid")) jsonObject.getString("uid") else ""
    val app_id = if (jsonObject.containsKey("app_id")) jsonObject.getString("app_id") else ""
    val device_id = if (jsonObject.containsKey("device_id")) jsonObject.getString("device_id") else ""
    val ip = if (jsonObject.containsKey("ip")) jsonObject.getString("ip") else ""
    val last_page_id = if (jsonObject.containsKey("last_page_id")) jsonObject.getString("last_page_id") else ""
    val page_id = if (jsonObject.containsKey("page_id")) jsonObject.getString("page_id") else ""
    val next_page_id = if (jsonObject.containsKey("next_page_id")) jsonObject.getString("next_page_id") else ""
    Page(uid, app_id, device_id, ip, last_page_id, page_id, next_page_id)
  }

  /**
    * 根据ip得出相应省份，展示出top3省份的点击数，需要根据历史数据累加
    *
    * @param ssc
    * @param client
    * @param filterDs
    */
  private def ipForTop3Province(ssc: StreamingContext, filterDs: DStream[Page]) = {
    //广播地址库
    //    ssc.sparkContext.addFile(this.getClass.getResource("/ip2region.db").getPath) //local
    //    ssc.sparkContext.addFile("G:\\IDEAProject\\eduction_online\\new_ed_online_real\\src\\main\\resources\\ip2region.db") //local
    ssc.sparkContext.addFile("hdfs://nameservice1/user/dog/resource/ip2region.db") //yarn
    //获取 当前批次 ip ds(privinceid,1)
    val ipDs: DStream[(String, Long)] = filterDs.mapPartitions(partitions => {
      val dfFile: String = SparkFiles.get("ip2region.db")
      val config = new DbConfig()
      val ipsearch = new DbSearcher(config, dfFile)
      partitions.map(item => {
        val ip: String = item.ip
        //获取ip详情   中国|0|上海|上海市|有线通
        val province: String = ipsearch.memorySearch(ip).getRegion.split("\\|")(2)
        //根据省份，统计点击个数
        (province, 1L)
      })
    }).reduceByKey(_ + _)
    //mysql查询历史数据，跟当前数据进行合并并更新回mysql
    ipDs.foreachRDD(rdd => {
      //查询历史数据
      val ipSqlProxy = new SqlProxy
      val ipClient: Connection = DataSourceUtil.getConnection
      //这里直接在driver获取历史数据，地址的历史数据不多
      try {
        var history_data = new ArrayBuffer[(String, Long)]()
        ipSqlProxy.executeQuery(ipClient, "select province,num from tmp_city_num_detail", null, new QueryCallback {
          override def process(rs: ResultSet): Unit = {
            while (rs.next()) {
              val tuple: (String, Long) = (rs.getString(1), rs.getLong(2))
              history_data += tuple
            }
          }
        })
        //将历史数据转成RDD 跟 当前批次数据进行合并
        val history_rdd = ssc.sparkContext.makeRDD(history_data)
        val resultRDD: RDD[(String, Long)] = history_rdd.fullOuterJoin(rdd).map(item => {
          val province: String = item._1
          val nums = item._2._1.getOrElse(0l) + item._2._2.getOrElse(0l)
          (province, nums)
        })
        resultRDD.foreachPartition(partitions => {
          val sqlProxy = new SqlProxy
          val client = DataSourceUtil.getConnection
          try {
            partitions.foreach(item => {
              //更新mysql数据，并返回最新结果数据用来求top n
              sqlProxy.executeUpdate(client,
                s"""
                   |insert into tmp_city_num_detail(province,num)
                   | values(?,?)
                   | on duplicate key update num=?
               """.stripMargin,
                Array(item._1, item._2, item._2))
            })
          } catch {
            case  e:Exception=>e.printStackTrace()
          }finally {
            sqlProxy.shutdown(client)
          }
        })

        val top3RDD: Array[(String, Long)] = resultRDD.sortBy(_._2,false).take(3)
        //写到mysql中去
        //先删除
        ipSqlProxy.executeUpdate(ipClient,
          s"""
             |truncate table top_city_num
           """.stripMargin,
          null)
        top3RDD.foreach(item => {
          ipSqlProxy.executeUpdate(ipClient,
            s"""
               |insert into top_city_num(province,num)
               | values(?,?)
           """.stripMargin,
            Array(item._1, item._2))
        })
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        ipSqlProxy.shutdown(ipClient)
      }
    })
  }

  /**
    * 计算转换率
    */
  def calcJumRate(sqlProxy: SqlProxy, client: Connection) = {
    var page1_num = 0L
    var page2_num = 0L
    var page3_num = 0L
    sqlProxy.executeQuery(client, "select num from page_jump_rate where page_id=?", Array(1), new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          page1_num += rs.getLong(1)
        }
      }
    })
    sqlProxy.executeQuery(client, "select num from page_jump_rate where page_id=?", Array(2), new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          page2_num += rs.getLong(1)
        }
      }
    })
    sqlProxy.executeQuery(client, "select num from page_jump_rate where page_id=?", Array(3), new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          page3_num += rs.getLong(1)
        }
      }
    })
    val nf = NumberFormat.getPercentInstance
    val page1ToPage2Rate = if (page1_num == 0) "0%" else nf.format(page2_num.toDouble / page1_num.toDouble)
    val page2ToPage3Rate = if (page2_num == 0) "0%" else nf.format(page3_num.toDouble / page2_num.toDouble)
    //更新回去
    sqlProxy.executeUpdate(client, "update page_jump_rate set jump_rate=? where page_id=?", Array(page1ToPage2Rate, 2))
    sqlProxy.executeUpdate(client, "update page_jump_rate set jump_rate=? where page_id=?", Array(page2ToPage3Rate, 3))
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
