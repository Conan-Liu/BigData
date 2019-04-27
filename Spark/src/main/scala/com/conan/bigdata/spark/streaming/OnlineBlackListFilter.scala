package com.conan.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * Created by Administrator on 2017/1/4.
  */
object OnlineBlackListFilter {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("OnlineBlackListFilter").setMaster("local[2]")
        val ssc = new StreamingContext(conf, Durations.seconds(10))
        ssc.sparkContext.setLogLevel("WARN")

        // 模拟黑名单列表， 这里简单用数组表示
        val blackList = Array(("hadoop", true), ("mahout", true))
        val blackListRDD = ssc.sparkContext.parallelize(blackList, 2)
        blackListRDD.cache()
        // 读取数据的格式如下
        // 日志  姓名
        val clickStream = ssc.socketTextStream("CentOS", 9999)

        val clickStreamFormatted = clickStream.map(ads => (ads.split("\\s+")(1), ads))
        // transform 函数允许里面进行RDD的相关操作， 然后返回一个DStream， 继续作为流处理
        clickStreamFormatted.transform(clickRDD => {
            val joinedBlackListRDD = clickRDD.leftOuterJoin(blackListRDD)
            // (key, (ads, true))
            joinedBlackListRDD.take(10)
            val validClicked = joinedBlackListRDD.filter(joinedItem => {
                if (joinedItem._2._2.getOrElse(false))
                    false
                else
                    true
            })
            validClicked.map(valid => {
                valid._2._1
            })
        }).print()

        ssc.start()
        ssc.awaitTermination()
    }
}


//Time: 1483498200000 ms
//Time: 1483498500000 ms
//Time: 1483498800000 ms
//Time: 1483499100000 ms