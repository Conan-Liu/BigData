package com.conan.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  */
object WindowWordCount {

    val ip = "192.168.1.8"
    val port = 9999

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("WindowWordCount")
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        ssc.sparkContext.setLogLevel("WARN")
        ssc.checkpoint("/user/root/temp/spark/checkpoint/windowwordcount")

        // 判定条件，不符合则异常退出
        // require(false,"hahahahaha")

        val lines = ssc.socketTextStream(ip, port)
        // 这里滑动窗口是 20s， 4倍的批次， 所以， 执行的时候， 是按照4倍的批次时间执行一次任务， 也就是20秒执行一次
        // 这样的话，等于多个批次的RDD合并成一个RDD来处理
        val result = lines.window(Seconds(5) * 30, Seconds(5) * 4).persist(StorageLevel.MEMORY_ONLY_SER)
        // checkpoint 里面调用了persist()方法，如果自己不写persist，还是会报错
        result.checkpoint(Seconds(5)*4)
        result.print()
        val wordCounts = result.flatMap(x => {
            println(x)
            x.split("\\s+")
        }).map((_, 1)).reduceByKey(_ + _)
        wordCounts.print()

        ssc.start()
        ssc.awaitTermination()
        ssc.stop(false,true)
    }
}