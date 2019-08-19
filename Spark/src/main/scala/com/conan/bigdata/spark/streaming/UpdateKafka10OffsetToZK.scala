package com.conan.bigdata.spark.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 这个 0.10 版本的目前还在实验阶段, 有些方法不合适, 某些api可能存在修改的风险, 不考虑这个方法
  * 这段代码没有执行成功
  *
  * 官方推荐使用这种 API , 将 offset保存到 kafka 中， 而不是第三方存储系统
  */
object UpdateKafka10OffsetToZK {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("UpdateKafka10OffsetToZK").setMaster("local[*]")
        val ssc = new StreamingContext(sparkConf, Seconds(10))
        ssc.sparkContext.setLogLevel("WARN")

        val Array(brokerList, groupId, topics) = Array("CentOS:9092", "test_group", "mulitkafkastreaming")

        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> brokerList,
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> groupId,
            "auto.offset.reset" -> "earliest",
            "enable.auto.commit" -> (false: java.lang.Boolean),
            "partition.assignment.strategy" -> "org.apache.kafka.clients.consumer.RangeAssignor"
        )
        val topicArray = Array(topics)

        // 在kafka中记录读取偏移量
        val kafkaStream = KafkaUtils.createDirectStream[String, String](
            ssc,
            // 位置策略，（可用的Executor上均匀分布）
            LocationStrategies.PreferConsistent,
            // 消费策略，（订阅的Topic集合）
            ConsumerStrategies.Subscribe[String, String](topicArray, kafkaParams))
        kafkaStream.foreachRDD(rdd => {
            // 获取该RDD对应的偏移量
            val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            // 拿出对应的数据
            rdd.foreachPartition(partition => {
                partition.foreach(record => {
                    println(record.key() + " : " + record.value())
                })
            })
            // 异步更新偏移量到Kafka中
            kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        })

        ssc.start()
        ssc.awaitTermination()
    }
}