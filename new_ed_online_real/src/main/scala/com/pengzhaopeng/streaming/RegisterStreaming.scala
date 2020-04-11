package com.pengzhaopeng.streaming

import java.lang
import java.sql.{Connection, ResultSet}

import com.pengzhaopeng.utils.{ConfigurationManager, DataSourceUtil, QueryCallback, SqlProxy}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * description 
  * author 鹏鹏鹏先森
  * date 2020/4/9 0:02
  * Version 1.0
  * 实时统计注册人数
  */
object RegisterStreaming {

  private val batchDuration = 3

  def main(args: Array[String]): Unit = {
    //设置基础参数
    System.setProperty("HADOOP_USER_NAME", "dog")

    //设置spark参数
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(batchDuration))
    val sparkContext: SparkContext = ssc.sparkContext

    //设置kafka参数
    val topics = Array(ConfigurationManager.getProperty("kafka.topic.qc.register"))
    val groupid = ConfigurationManager.getProperty("kafka.topic.qc.register.groupid")
    val kafkaMap: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> ConfigurationManager.getProperty("kafka.bootstrap.servers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )

    //设置 HA 的高可用
    sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    sparkContext.hadoopConfiguration.set("dfs.nameservices", "nameservice1")

    //查询mysql中是否有偏移量
    val sqlProxy = new SqlProxy()
    val client: Connection = DataSourceUtil.getConnection
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
    val queryOffsetSql: String =
      s"""
         |select
         |
         |from `offset_manager`
         |where `groupid`=$groupid
       """.stripMargin
    try {
      sqlProxy.executeQuery(client, queryOffsetSql, null, new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            val model = new TopicPartition(rs.getString(2), rs.getInt(3))
            val offset: Long = rs.getLong(4)
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

    //设置kafka消费的数据的参数 判断班底是否有偏移量 有则根据偏移量继续消费 无则重新消费
    val stream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty) {
      KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaMap))
    } else {
      KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaMap, offsetMap))
    }

    //清洗数据
    val filterRDD: DStream[(String, Int)] = stream
      .filter(item => {
        item.value().split("\t").length == 3
      })
      .mapPartitions(partitions => {
        partitions.map(item => {
          val line: String = item.value()
          val fields: Array[String] = line.split("\t")
          val app_name: String = fields(1) match {
            case "1" => "PC"
            case "2" => "APP"
            case "3" => "Other"
          }
          (app_name, 1)
        })
      })

    filterRDD.cache()
    //需求一：实时统计注册人数，批次为3秒一批，使用updateStateBykey算子计算历史数据和当前批次的数据总数
    appRegisterCounts(filterRDD)

    //需求二：每6秒统统计一次1分钟内的注册数据，不需要历史数据 提示:reduceByKeyAndWindow算子
//    appRigisterCountsBy1Minute(filterRDD)

    //处理完业务逻辑后手动提交 offset 维护到本地 mysql 中

    //优雅停止
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 实时统计注册人数，加历史数据
    */
  private def appRegisterCounts(filterRDD: DStream[(String, Int)]) = {

    filterRDD.updateStateByKey(updateFunc).print()
  }

  def updateFunc(values: Seq[Int], state: Option[Int]) = {
    //    var value = 0
    //    for (elem <- values) {
    //      value += elem
    //    }
    //本批次求和
    val currentCount: Int = values.sum
    //历史数据
    val previousCount: Int = state.getOrElse(0)
    Some(currentCount + previousCount)
  }

  /**
    * 每6秒统统计一次1分钟内的注册数据
    *
    * @param
    */
  private def appRigisterCountsBy1Minute(filterRDD: DStream[(String, Int)]) = {
    filterRDD.reduceByKeyAndWindow(
      (x: Int, y: Int) => x + y,
      Seconds(batchDuration * 20),
      Seconds(batchDuration)
    ).print()
  }
}
