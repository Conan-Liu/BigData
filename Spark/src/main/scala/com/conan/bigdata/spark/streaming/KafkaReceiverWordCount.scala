package com.conan.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark streaming 对接 Kafka
  * 方式一
  */
object KafkaReceiverWordCount {
    def main(args: Array[String]): Unit = {

        if (args.length != 4) {
            System.err.println("Usage: KafkaReceiverWordCount <zkQuorum> <group_id> <topics> <threads>")
            System.exit(1)
        }

        val sparkConf = new SparkConf()//.setAppName("KafkaReceiverWordCount").setMaster("local[*]")
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        ssc.sparkContext.setLogLevel("WARN")

        val Array(zkQuorum, group_id, topics, threads) = args

        val topicMap = topics.split(",").map((_, threads.toInt)).toMap

        // zkQuorum = CentOS:2181/kafka0901, group_id=test_group, topic=streamingtopic, threads=1
        // 一定要注意该kafka所在的zk地址， 否则，不报错， 拿不到数据
        val message = KafkaUtils.createStream(ssc, zkQuorum, group_id, topicMap)
        // 查看message的数据结构长什么样, 打印 null 什么鬼
        //        message.map(_._1).print()

        val result = message.map(_._2).flatMap(_.split("\\s+")).map(x => (x, 1)).reduceByKey(_ + _)
        result.print()

        ssc.start()
        ssc.awaitTermination()
    }
}