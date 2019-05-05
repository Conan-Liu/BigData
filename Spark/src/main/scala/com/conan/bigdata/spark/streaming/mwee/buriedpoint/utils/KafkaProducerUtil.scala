package com.conan.bigdata.spark.streaming.mwee.buriedpoint.utils

import com.conan.bigdata.spark.streaming.mwee.buriedpoint.config.Config
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Created by Conan on 2019/5/5.
  */
object KafkaProducerUtil {

    private val producer = createKafkaProducer //val

    private def createKafkaProducer(): KafkaProducer[String, String] = {
        new KafkaProducer[String, String](Config.SINK_KAFKA_PARAMS)
    }

    def getInstance(): KafkaProducer[String, String] = {
        producer
    }

    /**
      * 将数据写入 kafka
      */
    def sendKafka(message: String): Unit = {
        this.getInstance().send(new ProducerRecord[String, String](Config.SINK_KAFKA_TOPIC, message))
        this.getInstance().close()
    }
}