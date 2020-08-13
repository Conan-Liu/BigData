package com.conan.bigdata.spark.streaming

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupDirs, ZKGroupTopicDirs, ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
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
          * topics     spark要消费的topic, 可支持消费多个topic, 逗号隔开, 如 kafkastreaming,mulitkafkastreaming
          */
        val Array(brokerList, zkQuorum, groupId, topics) = Array("CentOS:9092", "CentOS:2181", "test_group", "mulitkafkastreaming")

        val kafkaParams = Map[String, String](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
            ConsumerConfig.GROUP_ID_CONFIG -> groupId,
            "auto.commit.enable" -> "false",
            "auto.offset.reset" -> "smallest" // 设定如果没有offset指定的时候， 从什么地方开始消费，默认最新
        )

        val topicSet = topics.split(",").map(_.trim).toSet
        val zkClient = new ZkClient(zkQuorum, Integer.MAX_VALUE, Integer.MAX_VALUE, null)
        // ZKStringSerializer)
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
        for (o <- fromOffsets) {
            println(o._1.topic + "\t" + o._1.partition + "\t" + o._2)
        }

        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
        val kafkaStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)


        //  ===========================这是代码上的一个分水岭========================
        //  任务启动的时候， 代码回去保存 offset 的 ZK 中查找一次， 来确定开始消费的 offset
        //  一旦正常运行后， 以后的每个批次就不再去 ZK 中拿offset， 因为正常运行的时候， kafka通过元数据Topic(__consumer_offsets)来维护offset，
        //  所以， 只要永远不挂机或重启， 一直消费下去， offset永远不会有问题， 但是一旦出现死机或重启， 这个维护的offset就丢了
        //  启动的时候， 会重新去 ZK 中获取 offset， 所以才需要没消费一个批次， 就保存一次offset， 避免发生意外情况重启重复消费
        //  以上的代码， 只会在启动的时候执行一次，  下面的代码在每个批次的时候重复执行， 无限循环

        //  综上所述： 只有启动的时候才会去 ZK 中拿offset， streaming正常运行是不用去ZK中获取offset的， 这个由kafka和streaming共同维护
        //  代码每消费一个批次，都要保存offset， 虽然正常运行时， 这个ZK中的offset用不上，
        //  但是如果出现意外的时候， 就可能从记录的offset地方开始启动消费
        //  这才是 ZK 保存 offset 的用途所在, 其它第三方保存offset， 也是这个道理

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

                // 下面代码仍然有效，只是版本冲突，先注释掉
                // ZkUtils.updatePersistentPath(zkClient, zkPath, o.untilOffset.toString)
            }
        })

        ssc.start()
        ssc.awaitTermination()
    }

}