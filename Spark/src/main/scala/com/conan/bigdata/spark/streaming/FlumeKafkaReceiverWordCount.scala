package com.conan.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark streaming 对接 Kafka
  * 方式一
  */
object FlumeKafkaReceiverWordCount {
    def main(args: Array[String]): Unit = {

        if (args.length != 4) {
            System.err.println("Usage: FlumeKafkaReceiverWordCount <zkQuorum> <group_id> <topics> <threads>")
            System.exit(1)
        }

        val sparkConf = new SparkConf().setAppName("FlumeKafkaReceiverWordCount").setMaster("local[*]")
        val ssc = new StreamingContext(sparkConf, Seconds(5))
//        ssc.sparkContext.setLogLevel("DEBUG")

        val Array(zkQuorum, group_id, topics, threads) = args

        val topicMap = topics.split(",").map((_, threads.toInt)).toMap

        val message = KafkaUtils.createStream(ssc, zkQuorum, group_id, topicMap)
        // 查看message的数据结构长什么样, 打印 null 什么鬼
//        message.map(_._1).print()

        val result = message.map(_._2).count()
        result.print()

        ssc.start()
        ssc.awaitTermination()
    }
}