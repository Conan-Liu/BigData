package com.conan.bigdata.spark.streaming

import java.io.{DataInput, DataInputStream}

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
import org.roaringbitmap.RoaringBitmap


object AccmulatorUVPV {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("AccmulatorUVPV").setMaster("local[*]")
        val ssc = new StreamingContext(sparkConf, Seconds(10))
        ssc.sparkContext.setLogLevel("WARN")

        val Array(brokerList, zkQuorum, groupId, topics) = Array("CentOS:9092", "CentOS:2181", "test_group", "mulitkafkastreaming")

        val kafkaParams = Map[String, String](
            "bootstrap.servers" -> brokerList,
            "group.id" -> groupId,
            "auto.commit.enable" -> "true",
            "auto.offset.reset" -> "smallest" // 设定如果没有offset指定的时候， 从什么地方开始消费，默认最新
        )
        val topicSet = topics.split(",").toSet
        val messageDirect = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)

        messageDirect.foreachRDD((rdd: RDD[(String, String)], timer: Time) => {
            ssc.sparkContext.textFile("",1)
        })

        // 停止
        ssc.start()
        ssc.awaitTermination()
    }

    def readBitMap(in:DataInputStream): RoaringBitmap = {
        val hdfsBitMap = new RoaringBitmap()
        hdfsBitMap.deserialize(in)
        hdfsBitMap
    }
}