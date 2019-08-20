package com.conan.bigdata.spark.streaming.flownotice.util

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

/**
  */
object KafkaClient extends Serializable{

    private def getKafkaProducer(): KafkaProducer[String, String] = {
        val properties = new Properties()
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOS:9092")
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
        properties.put(ProducerConfig.ACKS_CONFIG, "1")
        new KafkaProducer[String, String](properties)
    }


    def sendMessage(msg: String): Unit = {
        val producer = getKafkaProducer()
        producer.send(new ProducerRecord[String, String]("hellotopic", msg))
    }

}