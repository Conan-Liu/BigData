package com.conan.bigdata.spark.streaming

import com.conan.bigdata.spark.utils.Spark
import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Conan on 2019/4/28.
  * SparkStreaming 整合 Flume
  * flume 监控控制台输入， ， sparkstreaming来监听该端口
  * 这是 Streaming 主动去 Flume sink端拉去数据  Pull-based Approach using a Custom Sink
  * 这种方式有事务支持， 容错性较高， 可靠性， 都比第一种flume推送的机制要好， source过来到sink的数据， 被放在缓冲区
  * 生产优先考虑这个
  * 对应 flume_pull_streaming.conf
  */
object FlumePullWordCount {
    def main(args: Array[String]): Unit = {

        if (args.length != 2) {
            println("Need Two Parameters")
            System.exit(1)
        }

        val Array(hostname, port) = args
        //        val Array(hostname, port) = Array(Spark.HOST_NAME, "41414")

        //        val sparkConf = new SparkConf().setAppName("FlumePullWordCount").setMaster("local[2]")
        val sparkConf = new SparkConf() // 提交到集群上运行， 就不要指定name和master， submit是再指定
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        ssc.sparkContext.setLogLevel("WARN")

        val flumeStream = FlumeUtils.createPollingStream(ssc, hostname, port.toInt)
        // flume 发过来的基本数据单元是Event， 里面包含body和header， 所以需要多一步解析， 如map
        val result = flumeStream.map(x => new String(x.event.getBody.array()).trim)
            .flatMap(_.split("\\s+")).map(x => (x, 1)).reduceByKey(_ + _)
        result.print()

        ssc.start()
        ssc.awaitTermination()
    }


}