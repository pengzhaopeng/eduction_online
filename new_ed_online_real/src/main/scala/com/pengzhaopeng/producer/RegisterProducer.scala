package com.pengzhaopeng.producer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * description 
  * author 鹏鹏鹏先森
  * date 2020/4/8 0:22
  * Version 1.0
  */
object RegisterProducer {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
    val ssc = new SparkContext(sparkConf)
    //    println(this.getClass.getResource("/register.log").getPath)
    ssc.textFile("/user/dog/ods/register.log", 3)
      .foreachPartition(partition => {
        val props = new Properties()
        props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
        props.put("acks", "1")
        props.put("batch.size", "16384")
        props.put("linger.ms", "10")
        props.put("buffer.memory", "33554432")
        props.put("key.serializer",
          "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer",
          "org.apache.kafka.common.serialization.StringSerializer")

        val producer = new KafkaProducer[String, String](props)
        partition.foreach(item =>{
          val msg = new ProducerRecord[String,String]("qc_register",item)
          producer.send(msg)
        })
        producer.flush()
        producer.close()
      })
  }
}
