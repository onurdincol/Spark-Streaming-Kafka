Spark-Streaming-Kafka Connection

In this Test project, 4 different operations are available.


KafkaProducerTest : Produces MOCK_DATA.json file to the topic. Takes 2 arguments brokers and topics.
 Usage: KafkaProducerTest <brokers> <topics>
  <brokers> is a list of Kafka brokers for local usage localhost:9092
  <topics> is a list of kafka topics for local usage local_topic_name


KafkaConsumerTest : Consumes data from the Kafka topic and print. Takes 2 arguments brokers and topics.
 Usage: KafkaConsumerTest <brokers> <topics>
   <brokers> is a list of Kafka brokers for local usage localhost:9092
   <topics> is a list of kafka topics for local usage local_topic_name


KafkaConsumerOffsetInfo : Prints topic information for each partition in format;
  offset.topic, offset.partition, offset.fromOffset, offset.count(), offset.untilOffset
    For Example:
                    (mytargetTopic,0,0,334,334)
                    (mytargetTopic,1,0,316,316)
                    (mytargetTopic,2,0,350,350)

Usage: KafkaConsumerOffsetInfo <brokers> <topics>
  <brokers> is a list of Kafka brokers for local usage localhost:9092
  <topics> is a list of kafka topics for local usage local_topic_name


KafkaTopicToTopic : Consumes from the Source Kafka Topic and produce to Target Kafka Topic.
In this operation, I have also added two operations; making the JSON flatten and changing the date format(PST to UTC).

Usage: KafkaTopicToTopic <brokers> <source_topic> <target_topic>
  <brokers> is a list of Kafka brokers for local usage localhost:9092
  <source_topic> is a kafka topic to read from
  <target_topic> is a kafka topic to write to