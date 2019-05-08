package com.conan.bigdata.spark.streaming

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
  * Created by Administrator on 2019/5/7.
  */
object UpdateKafka08OffsetToZK {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("UpdateKafka08OffsetToZK").setMaster("local[*]")
        val ssc = new StreamingContext(sparkConf, Seconds(10))
        ssc.sparkContext.setLogLevel("WARN")

        /**
          * 直连方式
          * brokerList 就是spark要直连的broker地址
          * zkQuorum   就是spark把offset的偏移量保存到指定的zk中， 逗号隔开
          * groupId    spark消费所在的消费者组
          * topics     spark要消费的topic
          */
        val Array(brokerList, zkQuorum, groupId, topics) = Array("CentOS:9092", "CentOS:2181", "test_group", "mulitkafkastreaming")

        val kafkaParams = Map[String, String](
            "bootstrap.servers" -> brokerList,
            "group.id" -> groupId,
            "auto.commit.enable" -> "false",
            "auto.offset.reset" -> "smallest" // 设定如果没有offset指定的时候， 从什么地方开始消费，默认最新
        )
        val topicSet = topics.split(",").toSet
        // 创建一个 ZKGroupTopicDirs 对象,其实是指定往zk中写入数据的目录，用于保存偏移量
        // 前后两个topic的类型不一样， streaming需要set类型的， 这里是需要string类型
        val topicDirs = new ZKGroupTopicDirs(groupId, topics)
        // 获取zookeeper中的路径， 因为之前配置kafka的时候，指定的目录不在根目录，有/kafka前缀，看kafka的配置文件
        // 为了放在一个目录下好管理，所以需要特别加上前缀， 最好是新生成的zookeeper路径， 否则路径查找可能有问题
        val zkTopicPath = s"/kafka${topicDirs.consumerOffsetDir}"
        // 创建zookeeper client用于更新偏移量， 这是第三方的开源客户端， 如果直接传入zkQuorum，记录的offset会乱码
        // 用下面的方法可以编码zookeeper中乱码， 就是换个构造方法
        val zkClient = new ZkClient(zkQuorum, Integer.MAX_VALUE, Integer.MAX_VALUE, ZKStringSerializer)
        val children = zkClient.countChildren(zkTopicPath)
        var kafkaStream: InputDStream[(String, String)] = null
        var fromOffsets: Map[TopicAndPartition, Long] = Map()

        // 如果zookeeper返回的children大于0 ，代表曾经保存过offset， 从这里开始消费
        if (children > 0) {
            for (i <- 0 until children) {
                // 获得该分区， 在zookeeper中保存的offset
                // /g001/offsets/wordcount/0/10001
                val partitionOffset = zkClient.readData[String](s"${zkTopicPath}/${i}")
                val tp = TopicAndPartition(topics, i)
                // 将不同的partition和对应的offset，添加到fromOffsets，用于开始消费
                fromOffsets += (tp -> partitionOffset.toLong)
            }
            // 把kafka的原始数据， 进行第一波转换， transform成(topics, message)的Tuple， message就是kafka原来的数据
            val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
            // 通过kafka直连DStream（按照前面计算好的offset来继续消费数据）
            kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
        }
        // 如果children = 0 代表不存在offset， 只能从设定的地方开始消费
        else {
            kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)
        }
        // ========== 从上面截止这儿，已经完成从zookeeper里面读取offset， 并使用该offset成功创建了该DStream
        // ========== 下面开始把消费过后的新的offset保存到zookeeper里面
        // 获取偏移量范围
        var offsetRanges = Array[OffsetRange]()
        // 去kafka读取消息， DStream的Transform方法可以将当前批次的RDD获取出来
        val transformStream: DStream[(String, String)] = kafkaStream.transform(rdd => {
            // 得到该批次rdd对应kafka的offset， 该rdd是一个kafka的rdd，可以获取偏移量范围
            offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            rdd
        })
        // 数据计算逻辑
        val result: DStream[(String, Int)] = transformStream.map(_._2).flatMap(_.split("\\s+")).map(x => (x, 1)).reduceByKey(_ + _)

        // 数据遍历逻辑
        result.foreachRDD((rdd: RDD[(String, Int)], time: Time) => {
            println(s"============${time}==================================")
            rdd.foreachPartition(partition => {
                partition.foreach(record => {
                    println(record)
                })
            })

            for (o <- offsetRanges) {
                // 获取每个partition保存offset对应的zookeeper地址
                // 一个Topic有多个partition， 遍历更新partition的offset
                val zkPath = s"${zkTopicPath}/${o.partition}"
                // 设置了 auto.commit.enable 不自动提交， 需要自己手动提交offset
                ZkUtils.updatePersistentPath(zkClient, zkPath, o.fromOffset.toString)
            }
        })

        // 停止
        ssc.start()
        ssc.awaitTermination()
    }
}