package com.pengzhaopeng.streaming

import java.lang
import java.sql.{Connection, ResultSet}

import com.pengzhaopeng.bean.QzLog
import com.pengzhaopeng.utils.{ConfigurationManager, DataSourceUtil, QueryCallback, SqlProxy}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * description 做题统计
  * author 鹏鹏鹏先森
  * date 2020/4/12 23:48
  * Version 1.0
  */
object QzPointStreaming {

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
    val topics = Array(ConfigurationManager.getProperty("kafka.topic.qc.qz.log"))
    val groupid = ConfigurationManager.getProperty("kafka.topic.qc.qz.groupid")
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
    //过滤数据
    val filterDS: DStream[QzLog] = stream.filter(item =>
      item.value().split("\t").length == 6
    ).mapPartitions(partition =>
      partition.map(item => {
        val line = item.value()
        val arr = line.split("\t")
        val uid = arr(0) //用户id
        val courseid = arr(1) //课程id
        val pointid = arr(2) //知识点id
        val questionid = arr(3) //题目id
        val istrue = arr(4) //是否正确
        val createtime = arr(5) //创建时间
        QzLog(uid, courseid, pointid, questionid, istrue, createtime)
      })
    )
    filterDS.foreachRDD(rdd => {
      //获取相同用户 同一课程 同一知识点的所有题目
      val groupRdd: RDD[(String, Iterable[QzLog])] = rdd.groupBy(item => {
        item.uid + "-" + item.courserid + "-" + item.pointid
      })
      groupRdd.foreachPartition(f = partition => {
        //在分区下获取jdbc的连接
        val sqlProxy = new SqlProxy
        val client: Connection = DataSourceUtil.getConnection
        partition.foreach {
          //需求分析
          case (key, iters) => appQzAccuracyAnalysis(key, iters, sqlProxy, client)
        }
      })
    })


    //处理完业务逻辑 手动提交offset维护到本地 mysql 中
    stream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val sqlProxy = new SqlProxy
        val client: Connection = DataSourceUtil.getConnection

        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        val sql: String =
          s"""
             |replace into `offset_manager` (`groupid`,`topic`,`partition`,`untilOffset`)
             | values(?,?,?,?)
           """.stripMargin
        try {
          for (elem <- offsetRanges) {
            sqlProxy.executeUpdate(client, sql, Array(groupid, elem.topic, elem.partition.toString, elem.untilOffset))
          }
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(client)
        }
      }
    })

    //停止spark
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 去重后求正确率和完成率
    *
    * @param filterDS
    */
  def appQzAccuracyAnalysis(key: String, iters: Iterable[QzLog], sqlProxy: SqlProxy, client: Connection) = {
    //先去重 当前批次数据和历史数据进行去重 回写到历史表并记录个数
    val keys: Array[String] = key.split("-")
    val userid: Int = keys(0).toInt
    val courseid: Int = keys(1).toInt
    val pointid: Int = keys(2).toInt
    //对当前批次的数据下的 question 去重
    val questionids: Array[String] = iters.toArray.map(_.questionid).distinct
    //查询历史数据
    val historySql: String =
      s"""
         |select
         |  questionids
         |from qz_point_history
         |where userid=? and courseid=? and pointid=?
       """.stripMargin
    var questionid_history: Array[String] = Array()
    sqlProxy.executeQuery(client, historySql, Array(userid, courseid, pointid), new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          questionid_history = rs.getString(1).split(",")
        }
        rs.close()
      }
    })
    //历史数据和当前数据去重，拼接回写回msql
    val resultQuestionIds: Array[String] = questionid_history.union(questionids).distinct

  }
}
