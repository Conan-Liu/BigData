package com.conan.bigdata.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Conan on 2019/4/29.
  * Spark streaming 对接 Kafka
  * 方式一
  */
object FlumeKafkaDirectWordCount {

    // 传入的参数 : CentOS:9092 flumekafkastreaming
    def main(args: Array[String]): Unit = {

        if (args.length != 2) {
            System.err.println("Usage: FlumeKafkaDirectWordCount <broker> <topics>")
            System.exit(1)
        }

        val sparkConf = new SparkConf().setAppName("FlumeKafkaDirectWordCount").setMaster("local[*]")
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        ssc.sparkContext.setLogLevel("WARN")

        val Array(broker,topics) = args

        var kafkaParams = Map[String, String]()
        kafkaParams += ("bootstrap.servers" -> broker)

        val topicSet = topics.split(",").toSet

        val message = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)
        // 这个 Key 依旧是 null , 用的是kafka自带的kafka-console-producer.sh脚本， 这个脚本生成的KV对， k是null， 界面输入的内容，整个作为V
        // 如果自己写一个Producer来发送KV数据， 这个打印的是正常的K和V， 注意， 解析的时候数据类型一定要一致， 否则解析不出来， Integer用String来解析也不行
//        message.print()
        val result = message.map(_._2).count()
        result.print()

        ssc.start()
        ssc.awaitTermination()
    }
}