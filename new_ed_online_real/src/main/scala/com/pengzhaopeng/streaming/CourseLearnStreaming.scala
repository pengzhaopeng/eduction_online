package com.pengzhaopeng.streaming

import java.lang
import java.sql.{Connection, ResultSet}

import com.alibaba.fastjson.JSONObject
import com.pengzhaopeng.bean.LearnModel
import com.pengzhaopeng.streaming.PageStreaming.{batchDuration, calcJumRate}
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
  * @date 2020/4/28 14:02
  * @Version 1.0
  * @description 实时统计学员播放视频各时长
  */
object CourseLearnStreaming {
  private val batchDuration = 3

  def main(args: Array[String]): Unit = {
    //参数设置
    System.setProperty("HADOOP_USER_NAME", "dog")
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    //      .setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(batchDuration))
    //kafka参数
    val topics = Array(ConfigurationManager.getProperty("kafka.topic.qc.course.learn"))
    val groupid = ConfigurationManager.getProperty("kafka.topic.qc.course.learn.groupid")
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
    val filterDs: DStream[LearnModel] = stream.mapPartitions(partitions => {
      partitions.map(item => {
        val json = item.value()
        val jsonObject: JSONObject = ParseJsonData.getJsonData(json)
        val userId = jsonObject.getIntValue("uid")
        val cwareid = jsonObject.getIntValue("cwareid")
        val videoId = jsonObject.getIntValue("videoid")
        val chapterId = jsonObject.getIntValue("chapterid")
        val edutypeId = jsonObject.getIntValue("edutypeid")
        val subjectId = jsonObject.getIntValue("subjectid")
        val sourceType = jsonObject.getString("sourceType")
        val speed = jsonObject.getIntValue("speed")
        val ts = jsonObject.getLong("ts")
        val te = jsonObject.getLong("te")
        val ps = jsonObject.getIntValue("ps")
        val pe = jsonObject.getIntValue("pe")
        LearnModel(userId, cwareid, videoId, chapterId, edutypeId, subjectId, sourceType, speed, ts, te, ps, pe)
      })
    })

    //业务需求指标计算
    appAnalysisTarget(filterDs)

    //更新偏移量
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
    * 各项业务指标计算
    * 计算视频 有效时长  完成时长 总时长
    *
    * @param stream
    * @param filterDs
    */
  def appAnalysisTarget(filterDs: DStream[LearnModel]) = {
    filterDs.foreachRDD(rdd => {
      //rdd会用到多次 缓存rdd
      rdd.cache()

      //统计各用户播放视屏的 有效时长 完成时长 总时长
      rdd.groupBy(item => item.uid + "_" + item.cware_id + "_" + item.video_id).foreachPartition(f = partitions => { //这里的意思是：同一用户同一课程同一视屏去访问mysql只会访问一次。避免mysql访问多次，怕出现线程安全问题
        val sqlProxy = new SqlProxy
        val client = DataSourceUtil.getConnection
        try {
          partitions.foreach { case (key, iters) =>
            calcVideoTime(key, iters, sqlProxy, client) //计算视屏时长
          }
        } catch {
          case e:Exception => e.printStackTrace()
        }finally {
          sqlProxy.shutdown(client)
        }

      })

      //统计各章节下视屏播放总时长

      //统计各课件下视屏播放总时长

      //统计各辅导下的播放总时长

      //统计各播放平台下的播放总时长

      //统计同一科目下的播放总时长

    })
  }

  /**
    * 统计各用户播放视频 有效时长 完成时长 总时长
    * @param key
    * @param iters
    * @param sqlProxy
    * @param client
    */
  def calcVideoTime(key: String, iters: Iterable[LearnModel], sqlProxy: SqlProxy, client: Connection) = {

  }
}
