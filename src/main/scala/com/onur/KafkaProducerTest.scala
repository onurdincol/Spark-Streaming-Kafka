package com.onur

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}

/**
  * Created by Onur_Dincol on 10/6/2017.
  */
object KafkaProducerTest{
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: KafkaProducerTest <brokers> <topics>
           |  <brokers> is a list of Kafka brokers for local usage localhost:9092
           |  <topics> is a list of kafka topics for local usage local_topic_name
           |
        """.stripMargin)
      System.exit(1)
    }
    val Array(brokers, topics) = args
    val kafkaTopics = topics.split(",").toSet
    val props = new Properties()

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "LogProducer")

    val kafkaProducer: Producer[Nothing, String] = new KafkaProducer[Nothing, String](props)

    val Logs = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/MOCK_DATA.json")).getLines().toArray

    for(i <- 0 to Logs.length-1){
      val producerRecord = new ProducerRecord(topics, Logs(i))
      kafkaProducer.send(producerRecord)
    }
    kafkaProducer.close()
  }
}
