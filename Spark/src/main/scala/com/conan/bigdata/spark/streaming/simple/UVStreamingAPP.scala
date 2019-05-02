package com.conan.bigdata.spark.streaming.simple

import com.conan.bigdata.spark.streaming.simple.bean.ClickLog
import com.conan.bigdata.spark.streaming.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, StreamingContext}

/**
  * Created by Conan on 2019/5/2.
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

        val sparkConf = new SparkConf().setAppName("UVStreamingAPP").setMaster("local[*]")
        val ssc = new StreamingContext(sparkConf, Minutes(1))
        ssc.sparkContext.setLogLevel("WARN")

        val Array(zkQuorum, group_id, topics, threads) = args

        val topicMap = topics.split(",").map((_, threads.toInt)).toMap

        val messages = KafkaUtils.createStream(ssc, zkQuorum, group_id, topicMap)

        // 步骤一:  测试数据接收
        val result = messages.map(_._2).count()
        result.print()

        // 步骤二:  数据清洗, 数据格式 GET /class/112.html HTTP/1.1
        // 过滤掉不是class开头的网址
        val filterD = messages.map(message => {
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

        filterD.print()
        ssc.start()
        ssc.awaitTermination()
    }
}


