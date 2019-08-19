package com.conan.bigdata.spark.streaming.mwee.readjsoncalpvuv

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
//import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  */
object Driver {

    def main(args: Array[String]): Unit = {
//        val sparkConf = new SparkConf().setAppName("READJSON").setMaster("local[4]")
//
//        val ssc = new StreamingContext(sparkConf, Seconds(10))
//
//        val kafkaParams = Map[String, Object] (
//            "bootstrap.servers" -> "10.0.24.41:9092,10.0.24.42:9092,10.0.24.52:9092",
//            "key.deserializer" -> classOf[StringDeserializer],
//            "value.deserializer" -> classOf[StringDeserializer],
//            "group.id" -> "mweetest1",
//            "enable.auto.commit" -> false
//        )
//
//        val topics=Array("dataflow")
//
//        val streamRDD=KafkaUtils.createDirectStream[String,String](ssc)

    }

}
