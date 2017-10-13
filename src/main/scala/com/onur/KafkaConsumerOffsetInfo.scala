package com.onur

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Onur_Dincol on 10/6/2017.
  */
object KafkaConsumerOffsetInfo{
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: KafkaConsumerOffsetInfo <brokers> <topics>
           |  <brokers> is a list of Kafka brokers for local usage localhost:9092
           |  <topics> is a list of kafka topics for local usage local_topic_name
           |
        """.stripMargin)
      System.exit(1)
    }
    val Array(brokers, topics) = args
    val kafkaTopics = topics.split(",").toSet

    val ssc = new StreamingContext("local[*]", "KafkaConsumerOffsetInfo", Seconds(10))
    val kafkaDirectParams = Map("metadata.broker.list" -> brokers,
                                "auto.offset.reset" -> "smallest")
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaDirectParams, kafkaTopics)
    lines.foreachRDD((rdd,batchTime) => {
      if(!rdd.isEmpty()){  //Bu bize topic bilgilarini ve partition,offset bilgilerini verir
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          offsetRanges.foreach(offset => println(offset.topic,offset.partition, offset.fromOffset, offset.count(),
            offset.untilOffset))
      }
    })
    ssc.start()
    ssc.stop(true, true)
  }

}
