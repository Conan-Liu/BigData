package com.conan.bigdata.spark.streaming

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupDirs, ZKGroupTopicDirs, ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
  * Created by Administrator on 2019/5/7.
  * 消费多个 Topic , 保存在zookeeper中， 每次消费时，把zookeeper中的多个Topic的offset拿出来， createDirectStream支持多个Topic读取
  * 然后再更新回去即可
  */
object UpdateMultiKafka08OffsetToZK {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("UpdateMultiKafka08OffsetToZK").setMaster("local[*]")
        val ssc = new StreamingContext(sparkConf, Seconds(10))
        ssc.sparkContext.setLogLevel("WARN")

        /**
          * 直连方式
          * brokerList 就是spark要直连的broker地址
          * zkQuorum   就是spark把offset的偏移量保存到指定的zk中， 逗号隔开
          * groupId    spark消费所在的消费者组
          * topics     spark要消费的topic
          */
        val Array(brokerList, zkQuorum, groupId, topics) = Array("CentOS:9092", "CentOS:2181", "test_group", "kafkastreaming,mulitkafkastreaming")

        val kafkaParams = Map[String, String](
            "bootstrap.servers" -> brokerList,
            "group.id" -> groupId,
            "auto.commit.enable" -> "false",
            "auto.offset.reset" -> "smallest"
        )
        val topicSet = topics.split(",").map(_.trim).toSet
        val zkClient = new ZkClient(zkQuorum, Integer.MAX_VALUE, Integer.MAX_VALUE, ZKStringSerializer)
        var fromOffsets: Map[TopicAndPartition, Long] = Map()
        // 多个Topic， 那么遍历它们， 然后获取每个Topic下面每个Partition的offset
        topicSet.foreach(topicName => {
            val topicDir = new ZKGroupTopicDirs(groupId, topicName)
            val zkTopicPath = s"/kafka${topicDir.consumerOffsetDir}"
            //            println(s"读取时zookeeper中${topicName}的路径 : ${zkTopicPath}")
            // 从zookeeper里面获取broker里面一个Topic的分区个数，
            val children = zkClient.countChildren("/kafka" + ZkUtils.getTopicPartitionsPath(topicName))
            // 遍历这些个分区， 拿到每个分区的offset
            for (i <- 0 until children) {
                val tp = TopicAndPartition(topicName, i)
                // 如果一个Topic还没有被消费，那么在zookeeper中是没有消费者消费该分区的offset信息的， 所以不在该子节点
                if (zkClient.exists(zkTopicPath + s"/${i}")) {
                    // 如果有该节点表示，已经消费过了， 拿出消费offset
                    val partitionOffset = zkClient.readData[String](s"${zkTopicPath}/${i}")
                    fromOffsets += (tp -> partitionOffset.toLong)
                } else {
                    // 如果没有消费的话， 从头开始消费
                    fromOffsets += (tp -> 0l)
                }
            }
        })
        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
        val kafkaStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)

        var offsetRanges = Array[OffsetRange]()
        val transformStream: DStream[(String, String)] = kafkaStream.transform(rdd => {
            // 获取每个Topic上面每个分区的offset range， 就是消费范围
            offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            for (o <- offsetRanges) {
                println(s"${o.topic} - ${o.partition} : ${o.fromOffset} - ${o.untilOffset}")
            }
            rdd
        })

        // 多个Topic 当成一个DStream进行逻辑处理就行了
        val result: DStream[(String, Int)] = transformStream.map(_._2).flatMap(_.split("\\s+")).map(x => (x, 1)).reduceByKey(_ + _)

        result.foreachRDD((rdd: RDD[(String, Int)], time: Time) => {
            println(s"============${time}==================================")
            for (o <- offsetRanges) {
                println(s"START = ${o.topic} - ${o.partition} : ${o.fromOffset} - ${o.untilOffset}")
            }
            rdd.foreachPartition(partition => {
                // 这里不能打印offset， 因为这个RDD是进过上面那行单词计算后得到的， 分区数早已经变了， 不是建立连接时，RDD的分区和kafka分区一一对应的关系
                // val o: OffsetRange = offsetRanges(TaskContext.get().partitionId())
                // println(s"${o.topic} - ${o.partition} : ${o.fromOffset} - ${o.untilOffset}")
                partition.foreach(record => {
                    println(record)
                })
            })

            // 消费完了， 把不同Topic的不同分区offset信息，全部写回去
            val consumeDir = new ZKGroupDirs(groupId)
            for (o <- offsetRanges) {
                val zkPath = s"/kafka${consumeDir.consumerGroupDir}/offsets/${o.topic}/${o.partition}"
                // println(s"写入时zookeeper中${o.topic}对应的路径: " + zkPath)
                // println(s"END = ${o.topic} - ${o.partition} : ${o.fromOffset} - ${o.untilOffset}")
                // 注意这里应该是 o.untilOffset.toString， 网上有很多地方写的是 o.fromOffset.toString， 这是错的
                // 你想想， 消费完了， 肯定要把最后的offset记录下来，而不是开始的那个， 这样会重复消费
                ZkUtils.updatePersistentPath(zkClient, zkPath, o.untilOffset.toString)
            }
        })

        ssc.start()
        ssc.awaitTermination()
    }

}