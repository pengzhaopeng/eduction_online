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
import scala.collection.mutable.ArrayBuffer

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
    * 计算视频 有效时长  完成时长 总时长   有效时长和完成时长的区别在播放倍速
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
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(client)
        }

      })

      //统计各章节下视屏播放总时长
      rdd.mapPartitions(partition => {
        partition.map(item => {
          val totalTime = Math.ceil((item.te - item.ts) / 1000).toLong
          val key = item.chapter_id
          (key, totalTime)
        })
      }).reduceByKey(_ + _)
        .foreachPartition(partitions => {
          val sqlProxy = new SqlProxy
          val client: Connection = DataSourceUtil.getConnection
          try {
            partitions.foreach(item => {
              sqlProxy.executeUpdate(client,
                s"""
                   |insert into chapter_lear_detail(chapterid,totaltime)
                   | vaules(?,?)
                   | on duplicate key update totaltime=totaltime+?
                 """.stripMargin,
                Array(item._1, item._2, item._2))
            })
          } catch {
            case e: Exception => e.printStackTrace()
          } finally {
            sqlProxy.shutdown(client)
          }
        })

      //统计各课件下视屏播放总时长
      rdd.mapPartitions(partitions => {
        partitions.map(item => {
          val totalTime = Math.ceil((item.te - item.ts) / 1000).toLong
          val key = item.cware_id
          (key,totalTime)
        })
      }).reduceByKey(_+_)
        .foreachPartition(partitons =>{
          val sqlProxy = new SqlProxy
          val client: Connection = DataSourceUtil.getConnection
          try {
            partitons.foreach(item => {
              sqlProxy.executeUpdate(client,
                s"""
                  |insert into cwareid_learn_detail(cwareid,totaltime)
                  | values(?,?)
                  | on duplicate key update totaltime=totaltime+?
               """.stripMargin,
                Array(item._1,item._2,item._2))
            })
          } catch {
            case e:Exception => e.printStackTrace()
          } finally {
            sqlProxy.shutdown(client)
          }
        })

      //统计辅导下的总播放时长
      rdd.mapPartitions(partitions => {
        partitions.map(item => {
          val totaltime = Math.ceil((item.te - item.ts) / 1000).toLong
          val key = item.edutype_id
          (key, totaltime)
        })
      }).reduceByKey(_ + _).foreachPartition(partitions => {
        val sqlProxy = new SqlProxy()
        val client = DataSourceUtil.getConnection
        try {
          partitions.foreach(item => {
            sqlProxy.executeUpdate(client, "insert into edutype_learn_detail(edutypeid,totaltime) values(?,?) on duplicate key " +
              "update totaltime=totaltime+?", Array(item._1, item._2, item._2))
          })
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(client)
        }
      })

      //统计同一资源平台下的总播放时长
      rdd.mapPartitions(partitions => {
        partitions.map(item => {
          val totaltime = Math.ceil((item.te - item.ts) / 1000).toLong
          val key = item.source_type
          (key, totaltime)
        })
      }).reduceByKey(_ + _).foreachPartition(partitions => {
        val sqlProxy = new SqlProxy()
        val client = DataSourceUtil.getConnection
        try {
          partitions.foreach(item => {
            sqlProxy.executeUpdate(client, "insert into sourcetype_learn_detail (sourcetype_learn,totaltime) values(?,?) on duplicate key " +
              "update totaltime=totaltime+?", Array(item._1, item._2, item._2))
          })
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(client)
        }
      })
      // 统计同一科目下的播放总时长
      rdd.mapPartitions(partitions => {
        partitions.map(item => {
          val totaltime = Math.ceil((item.te - item.ts) / 1000).toLong
          val key = item.subject_id
          (key, totaltime)
        })
      }).reduceByKey(_ + _).foreachPartition(partitons => {
        val sqlProxy = new SqlProxy()
        val clinet = DataSourceUtil.getConnection
        try {
          partitons.foreach(item => {
            sqlProxy.executeUpdate(clinet, "insert into subject_learn_detail(subjectid,totaltime) values(?,?) on duplicate key " +
              "update totaltime=totaltime+?", Array(item._1, item._2, item._2))
          })
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(clinet)
        }
      })

    })
  }

  /**
    * 统计各用户播放视频 有效时长 完成时长 总时长
    *
    * @param key
    * @param iters
    * @param sqlProxy
    * @param client
    */
  def calcVideoTime(key: String, iters: Iterable[LearnModel], sqlProxy: SqlProxy, client: Connection) = {
    val keys = key.split("_")
    val userId = keys(0).toInt
    val cwareId = keys(1).toInt
    val videoId = keys(2).toInt
    //1、拿到历史数据（同一用户同一课程同一个视屏下的的所有观看记录区间）
    var interval_history = ""
    sqlProxy.executeQuery(client,
      s"""
         |select
         | play_interval
         |from video_interval
         |where userid=? and cwareid=? and videoid=?
       """.stripMargin,
      Array(userId, cwareId, videoId),
      new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            interval_history = rs.getString(1)
          }
        }
      })

    var effective_duration_sum = 0l //有效总时长
    var complete_duration_sum = 0l //完成总时长
    var cumulative_duration_sum = 0l //播放总时长

    //2、根据历史数据和当前批次数据进行区间去重（这里要区分是否有历史数据）
    val learnList: List[LearnModel] = iters.toList.sortBy(item => item.ps) //转成list 并根据开始区间升序排序
    learnList.foreach(item => {
      if (StringUtil.isEmpty(interval_history)) {
        //没有历史区间
        val play_interval = item.ps + "_" + item.pe //有效区间
        val complate_duration = item.pe - item.ps //完成时长
        val effective_duration = Math.ceil((item.te - item.ts) / 1000) //有效时长
        effective_duration_sum += effective_duration.toLong
        cumulative_duration_sum += effective_duration.toLong
        cumulative_duration_sum += complate_duration
        interval_history = play_interval
      } else {
        //有历史区间

        //累计时长
        val cumulative_duration = Math.ceil((item.te - item.ts) / 1000)
        cumulative_duration_sum += cumulative_duration.toLong

        //合并计算区间
        //计算完成时长
        val tuple = getEffectiveInterval(interval_history, item.ps, item.pe)
        val complete_duration = tuple._1 //获取本次实际有效完成时长
        complete_duration_sum += complete_duration
        interval_history = tuple._2 //整合后的所有区间

        //计算有效时长
        val effective_duration = Math.ceil((item.te - item.ts) / 1000) / (item.pe - item.ps) * complete_duration
        effective_duration_sum += effective_duration.toLong

      }
    })

    //3、历史数据和本批次数据合并处理完更新会mysql 区间历史表+用户播放记录表
    sqlProxy.executeUpdate(client,
      s"""
         |insert into video_interval(userid,cwareid,videoid,play_interval)
         | values(?,?,?,?)
         | on duplicate key update play_interval=?
       """.stripMargin,
      Array(userId, cwareId, videoId, interval_history, interval_history)
    )
    sqlProxy.executeUpdate(client,
      s"""
         |insert into video_learn_detail(userid,cwareid,videoid,totaltime,effecttime,completetime)
         | values(?,?,?,?,?,?)
         | on duplicate key update totaltime=totaltime+?, effecttime=effecttime+?, completetime=completetime+?
       """.stripMargin,
      Array(userId, cwareId, videoId, cumulative_duration_sum, effective_duration_sum, complete_duration_sum, cumulative_duration_sum, effective_duration_sum, complete_duration_sum))
  }

  /**
    * 计算有效区间和完成时长
    *
    * @param interval_history
    * @param ps
    * @param pe
    */
  def getEffectiveInterval(interval_history: String, start: Int, end: Int) = {
    //该区间的有效完成时间，用来叠加求总和，需要和历史区间去重再求总，若没有交集，则直接就是 end - start
    var effective_duration = end - start
    //先将所有区间段排序，先排首，首相同就排尾，让输出结果有序
    val array: Array[String] = interval_history.split(",").sortBy(item => (item.split("-")(0).toInt, item.split("-")(1).toInt))
    var isChangeDuration = false //是否对有效时间进行了修改，也就是是否进行了区间重组

    //拿当前区间和历史区间去循环对比，匹配到了就跳出循环，分5种情况 (这个循环用来修改历史array的区间和计算当前区间的有效完成时间)
    import scala.util.control.Breaks._
    breakable {
      for (i <- array.indices) {
        //获取这段的开始播放和结束播放区间
        var historyStart = 0
        var historyEnd = 0
        val item = array(i)
        try {
          historyStart = item.split("-")(0).toInt
          historyEnd = item.split("-")(1).toInt
        } catch {
          case e: Exception => throw new Exception("error array:" + array.mkString(","))
        }

        //分五种情况 其中4种情况存在交集 另外一种不存在交集直接叠加
        if (start >= historyStart && end <= historyEnd) {
          //1、当前区间被已有的数据包含，此次区间无效不计
          effective_duration = 0
          isChangeDuration = true
          break()
        } else if (start <= historyStart && end > historyStart && end < historyEnd) {
          //2、和已有区间左侧有交集
          effective_duration = effective_duration - (end - historyStart)
          isChangeDuration = true
          array(i) = start + "-" + historyEnd
          break()
        } else if (start > historyStart && start < historyEnd && end > historyEnd) {
          //3、和已有区间右侧存在交集
          effective_duration = effective_duration - (historyEnd - start)
          isChangeDuration = true
          break()
        } else if (start < historyStart && end > historyEnd) {
          //4、现区间完全覆盖老区间，老区间就失效
          effective_duration = effective_duration - (historyEnd - historyStart)
          isChangeDuration = true
          break()
        }
      }
    }

    //拿到更新后的array 重组区间并返回
    // (这里经过上面的区间修改还可能会存在有交集的区间 比如原本有 10-30，40-60，重新一条新的变成10-50，40-60，就有交集了)
    val result: String = isChangeDuration match {
      case false => { //没有修改原 array 没有交集 直接新增
        //没有修改原array 没有交集 进行新增
        val distinctArray2 = ArrayBuffer[String]()
        distinctArray2.appendAll(array)
        distinctArray2.append(start + "-" + end)
        val distinctArray = distinctArray2.distinct.sortBy(a => (a.split("-")(0).toInt, a.split("-")(1).toInt))
        val tmpArray = ArrayBuffer[String]()
        tmpArray.append(distinctArray(0))
        for (i <- 1 until distinctArray.length) {
          val item = distinctArray(i).split("-")
          val tmpItem = tmpArray(tmpArray.length - 1).split("-")
          val itemStart = item(0)
          val itemEnd = item(1)
          val tmpItemStart = tmpItem(0)
          val tmpItemEnd = tmpItem(1)
          if (tmpItemStart.toInt < itemStart.toInt && tmpItemEnd.toInt < itemStart.toInt) {
            //没有交集
            tmpArray.append(itemStart + "-" + itemEnd)
          } else {
            //有交集
            val resultStart = tmpItemStart
            val resultEnd = if (tmpItemEnd.toInt > itemEnd.toInt) tmpItemEnd else itemEnd
            tmpArray(tmpArray.length - 1) = resultStart + "-" + resultEnd
          }
        }
        val play_interval = tmpArray.sortBy(a => (a.split("-")(0).toInt, a.split("-")(1).toInt)).mkString(",")
        play_interval
      }
      case true => {
        //修改了原array 进行区间重组
        val distinctArray = array.distinct.sortBy(a => (a.split("-")(0).toInt, a.split("-")(1).toInt))
        val tmpArray = ArrayBuffer[String]()
        tmpArray.append(distinctArray(0))
        for (i <- 1 until distinctArray.length) {
          val item = distinctArray(i).split("-")
          val tmpItem = tmpArray(tmpArray.length - 1).split("-")
          val itemStart = item(0)
          val itemEnd = item(1)
          val tmpItemStart = tmpItem(0)
          val tmpItemEnd = tmpItem(1)
          if (tmpItemStart.toInt < itemStart.toInt && tmpItemEnd.toInt < itemStart.toInt) {
            //没有交集
            tmpArray.append(itemStart + "-" + itemEnd)
          } else {
            //有交集
            val resultStart = tmpItemStart
            val resultEnd = if (tmpItemEnd.toInt > itemEnd.toInt) tmpItemEnd else itemEnd
            tmpArray(tmpArray.length - 1) = resultStart + "-" + resultEnd
          }
        }
        val play_interval = tmpArray.sortBy(a => (a.split("-")(0).toInt, a.split("-")(1).toInt)).mkString(",")
        play_interval
      }
    }

    (effective_duration, result)
  }
}
