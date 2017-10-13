package com.onur

import java.util.Properties

import com.onur.util.{JsonOperations, TimeOperations}
//import com.onur.util.{JsonOperations, TimeOperations}
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json.JSONObject

/**
  * Created by Onur_Dincol on 10/6/2017.
  */
object KafkaTopicToTopic{
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println(
        s"""
           |Usage: KafkaTopicToTopic <brokers> <topics>
           |  <brokers> is a list of Kafka brokers for local usage localhost:9092
           |  <source_topic> is a kafka topic to read from
           |  <target_topic> is a kafka topic to write to
        """.stripMargin)
      System.exit(1)
    }
    val Array(brokers, sourceTopic, targetTopic) = args
    val sparkConf = new SparkConf().setAppName("KafkaTopicToTopic").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    val kafkaDirectParams = Map("metadata.broker.list" -> brokers,
                                "auto.offset.reset" -> "smallest")
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaDirectParams, Set(sourceTopic)).map(_._2)
    lines.foreachRDD{ rdd =>
      if(!rdd.isEmpty()){
        rdd.foreach { row =>
          val kafkaProducer: Producer[Nothing, String] = new KafkaProducer[Nothing, String](getProps(brokers))
          val jsonOperations = new JsonOperations
          val timeOperations = new TimeOperations
          val json = new JSONObject(jsonOperations.makeJsonFlatten(row))
          json.put("start_date",timeOperations.changePSTtoUTC(json.get("start_date").toString()))
          json.put("info.first_name",json.get("info.first_name").toString().toUpperCase)
          val producerRecord = new ProducerRecord(targetTopic, json.toString())
          kafkaProducer.send(producerRecord)
        }
      }
    }
    lines.print()
    ssc.start()
    ssc.awaitTermination()
  }

  def getProps(brokers : String) : Properties = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "TopicProducer")
    props
  }
}



