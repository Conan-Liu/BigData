package com.conan.bigdata.spark.streaming

import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010._

/**
  * 订阅模式下，sparkstreaming createDirectStream只能启动一个消费者线程？
  * 虽然Task数目和Topic的Partition数量一样，但是消费却是单线程？
  */
object MultiConsumerWordCount {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setAppName("MultiConsumerWordCount").setMaster("local[4]")
                .set("spark.streaming.kafka.consumer.cache.enabled", "false")
        val sc: SparkContext = SparkContext.getOrCreate(sparkConf)

        sc.setLogLevel("WARN")

        val ssc = new StreamingContext(sc, Seconds(10))

        val Array(brokerList, zkQuorum, groupId, topics) = Array("localhost:9092", "localhost:2181/kafka", "group_id_1", "hello")

        val kafkaParams = Map[String, Object](
            BOOTSTRAP_SERVERS_CONFIG -> brokerList,
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> groupId,
            // CLIENT_ID_CONFIG -> "haha_id", // 不能配置client_id，否则InstanceAlreadyExistsException: kafka.consumer:type=app-info,id=haha_id
            "auto.offset.reset" -> "earliest",
            "enable.auto.commit" -> (false: java.lang.Boolean),
            "partition.assignment.strategy" -> "org.apache.kafka.clients.consumer.RangeAssignor" // Kafka发生rebalance时的assign策略，默认是RangeAssignor
        )

        val topicArray = Array(topics)

        val kafkaStream = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](topicArray, kafkaParams)
        )

        /**
          * object not serializable (class: org.apache.kafka.clients.consumer.ConsumerRecord, value: ConsumerRecord
          * 不可以直接print()，因为print方法是Driver端执行，而数据是在Executor上执行计算，如果执行print方法，Driver会把Executor的数据拉取过来再执行print，但是ConsumerRecord是不可序列化的，所以报错
          * 1. kafkaStream.print()，不可以执行，ConsumerRecord不可序列化
          * 2. kafkaStream.map(_.value()).print()，可以执行，String是可以序列化的
          */

        kafkaStream.foreachRDD(rdd => {
            val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            rdd.foreachPartition(partition => {
                partition.foreach(record => {
                    println(s"sparkstreaming: ${record.key()} -> ${record.value()}")
                })
            })
            kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        })

        ssc.start()
        ssc.awaitTermination()
    }
}
