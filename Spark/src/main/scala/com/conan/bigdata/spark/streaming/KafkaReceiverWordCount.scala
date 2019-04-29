package com.conan.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Conan on 2019/4/29.
  * Spark streaming 对接 Kafka
  * 方式一
  */
object KafkaReceiverWordCount {
    def main(args: Array[String]): Unit = {

        if (args.length != 4) {
            System.err.println("Usage: KafkaReceiverWordCount <zkQuorum> <group_id> <topics> <threads>")
            System.exit(1)
        }

        val sparkConf = new SparkConf().setAppName("KafkaReceiverWordCount").setMaster("local[2]")
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        ssc.sparkContext.setLogLevel("WARN")

        val Array(zkQuorum, group_id, topics, threads) = args

        val topicMap = topics.split(",").map((_, threads.toInt)).toMap

        // TODO... 这个没有接收到数据怎么回事？ 难道是kafka的版本不兼容？
        val message = KafkaUtils.createStream(ssc, zkQuorum, group_id, topicMap)
        // 查看message的数据结构长什么样
        message.print()

        val result = message.map(_._2).flatMap(_.split("\\s+")).map(x => (x, 1)).reduceByKey(_ + _)
        print()

        ssc.start()
        ssc.awaitTermination()
    }
}