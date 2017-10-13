package com.onur

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Onur_Dincol on 10/6/2017.
  */
object KafkaConsumerTest {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: KafkaConsumerTest <brokers> <topics>
           |  <brokers> is a list of Kafka brokers for local usage localhost:9092
           |  <topics> is a list of kafka topics for local usage local_topic_name
           |
        """.stripMargin)
      System.exit(1)
    }
    val Array(brokers, topics) = args
    val kafkaTopics = topics.split(",").toSet
    val sparkConf = new SparkConf().setAppName("KafkaConsumerTest").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    val kafkaDirectParams = Map("metadata.broker.list" -> brokers,
                                "auto.offset.reset" -> "smallest")
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaDirectParams, kafkaTopics)
    lines.foreachRDD{rdd =>
      if(!rdd.isEmpty()){
        val newrdd = rdd.map(_._2)
        println(newrdd.count() + " Records consumed.")
        newrdd.foreach(println)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}

