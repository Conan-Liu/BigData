package com.conan.bigdata.spark.streaming.simple

import com.conan.bigdata.spark.streaming.simple.bean.{ClickLog, CourseClickCount, CourseSearchClickCount}
import com.conan.bigdata.spark.streaming.simple.dao.{CourseClickCountDAO, CourseSearchClickCountDAO}
import com.conan.bigdata.spark.streaming.utils.DateUtils
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
  *
  * 使用 SparkStreaming 处理kafka来的用户行为日志数据
  */
object UVStreamingAPP {

    // 传入参数 ： CentOS:2181/kafka test_group_uv flumekafkastreaming 1
    def main(args: Array[String]): Unit = {

        if (args.length != 4) {
            System.err.println("Usage: UVStreamingAPP <zkQuorum> <group_id> <topics> <threads>")
            System.exit(1)
        }

        val sparkConf = new SparkConf()//.setAppName("UVStreamingAPP").setMaster("local[*]")
        val ssc = new StreamingContext(sparkConf, Minutes(1))
//        ssc.sparkContext.setLogLevel("WARN")

        // 1. streaming 通过Receiver来接收数据
//        val Array(zkQuorum, group_id, topics1, threads) = Array("CentOS:2181/kafka", "test_group_uv", "flumekafkastreaming", "1")
//        val topicMap = topics1.split(",").map((_, threads.toInt)).toMap
//        val messages = KafkaUtils.createStream(ssc, zkQuorum, group_id, topicMap)

        // 2. streaming 直连kafka读取数据
        val Array(broker, topics2) = Array("CentOS:9092", "flumekafkastreaming")
        var kafkaParams = Map[String, String]()
        kafkaParams += ("bootstrap.servers" -> broker)
        val topicSet = topics2.split(",").toSet
        val messageDirect = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)

        // 步骤一:  测试数据接收
        val result = messageDirect.map(_._2).count()
        result.print()

        // 步骤二:  数据清洗, 数据格式 GET /class/112.html HTTP/1.1
        // 过滤掉不是class开头的网址
        val filterD = messageDirect.map(message => {
            val msgSplits = message._2.split("\t")
            val url = msgSplits(2).split(" ")(1)
            var courseId = 0

            // 拿到课程编号, 不是class开头的默认为 0, 以便过滤
            if (url.startsWith("/class")) {
                val courseHtml = url.split("/")(2)
                courseId = courseHtml.substring(0, courseHtml.lastIndexOf(".")).toInt
            }

            // ClickLog(ip:String,time:String,courseId:Int,statusCode:Int,referer:String)
            ClickLog(msgSplits(0), DateUtils.parseToMinute(msgSplits(1)), courseId, msgSplits(3).toInt, msgSplits(4))
        }).filter(_.courseId != 0)

        // 写入hbase, 采用累加的方式， 统计今天到现在为止的全部PV量
        val hbaseD = filterD.map(x => {
            // 计算这个批次的PV
            (x.time.substring(0, 8) + "_" + x.courseId, 1)
        }).reduceByKey(_ + _)

        hbaseD.foreachRDD(rdd => {
            rdd.foreachPartition(partition => {
                val list = new ListBuffer[CourseClickCount]
                partition.foreach(record => {
                    list.append(CourseClickCount(record._1, record._2))
                })

                CourseClickCountDAO.saveToHbase(list)
            })
        })

        // 统计从搜索引擎过来的pv量
        val searchHbaseD = filterD.map(x => {
            val hostSplit = x.referer.split("/")
            var host = ""
            if (hostSplit.length >= 3) {
                host = hostSplit(2).replace(".", "_")
            }
            (host, x.time, x.courseId)
        }).filter(_._1 != "").map(x => (x._2.substring(0, 8) + "_" + x._1 + "_" + x.x._3, 1))
            .reduceByKey(_ + _)

        searchHbaseD.foreachRDD(rdd => {
            rdd.foreachPartition(partition => {
                val list = new ListBuffer[CourseSearchClickCount]
                partition.foreach(record => {
                    list.append(CourseSearchClickCount(record._1, record._2))
                })

                CourseSearchClickCountDAO.saveToHbase(list)
            })
        })

        ssc.start()
        ssc.awaitTermination()
    }
}


