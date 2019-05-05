package com.conan.bigdata.spark.streaming.mwee.buriedpoint.config

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

/**
  * Created by Administrator on 2019/5/5.
  */
object Config {

    val prop = new Properties()
    prop.load(this.getClass.getClassLoader.getResourceAsStream("config.properties"))

    /**
      * source kafka params
      */
    val SOURCE_KAFKA_PARAMS = Map[String, Object](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> prop.getProperty("source.kafka.bootstrap.servers"),
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
        ConsumerConfig.GROUP_ID_CONFIG -> prop.getProperty("source.kafka.group.id"),
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false:java.lang.Boolean)
    )

    /**
      * source kafka topic
      */
    val SOURCE_KAFKA_TOPIC=prop.getProperty("source.kafka.topic")

    /**
      * sink kafka
      */
    val SINK_KAFKA_PARAMS = new Properties()
    SINK_KAFKA_PARAMS.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, prop.getProperty("sink.kafka.bootstrap.servers"))
    SINK_KAFKA_PARAMS.put(ProducerConfig.CLIENT_ID_CONFIG, prop.getProperty("sink.kafka.client.id"))
    SINK_KAFKA_PARAMS.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer]) //序列化
    SINK_KAFKA_PARAMS.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

    /**
      * sink kafka topic
      */
    val SINK_KAFKA_TOPIC = prop.getProperty("sink.kafka.topic")

    /**
      * sink hdfs
      */
    val SINK_HDFS_PATH = prop.getProperty("sink.hdfs.path")
}