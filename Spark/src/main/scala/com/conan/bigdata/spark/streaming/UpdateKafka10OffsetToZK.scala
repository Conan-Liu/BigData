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

        // 在kafka中记录读取偏移量，正常情况下不应该使用Subscribe模式，而需要Assign来自己根据保存的offset自定义消费，避免rebalance
        // val topicPartition = new Array[TopicPartition](new TopicPartition("topic", 0))
        // val offsets = Map[TopicPartition, Long](new TopicPartition("topic", 0) -> 0)

        // val strategy = ConsumerStrategies.Assign[String, String](topicPartition, kafkaParams, offsets)
        val kafkaStream = KafkaUtils.createDirectStream[String, String](
            ssc,
            // 位置策略，（可用的Executor上均匀分布）
            LocationStrategies.PreferConsistent,
            // 消费策略，（订阅的Topic集合）
            ConsumerStrategies.Subscribe[String, String](topicArray, kafkaParams)
            // strategy
        )

        var offsetRanges = Array[OffsetRange]()
        val filterStream = kafkaStream.transform(rdd => {
            offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            rdd
        })

        filterStream.foreachRDD(rdd => {
            // 获取该RDD对应的偏移量，因为这里kafkaStream没有转换还保持着原来的分区情况，所以可以获取偏移量，如果有任何转换，这里都不能获取，会报错，推荐使用transform来获取offset，如上
            val offsetRangesTest = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
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