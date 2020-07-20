package com.conan.bigdata.spark.streaming

import com.conan.bigdata.spark.streaming.utils.{MyStreamingListener, Tools}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}

/**
  * nc -lk 9999
  * 接收 9999 端口传来的数据， 并处理
  */
object NetworkWordCount {

    private val log: Logger = LoggerFactory.getLogger("NetworkWordCount")

    def main(args: Array[String]) {
        //设置日志输出级别，省的控制台全是没用的日志
        org.apache.log4j.Logger.getLogger("org.apache.spark").setLevel(org.apache.log4j.Level.OFF)
        org.apache.log4j.Logger.getLogger("java.lang").setLevel(org.apache.log4j.Level.OFF)
        org.apache.log4j.Logger.getLogger("org.spark_project").setLevel(org.apache.log4j.Level.OFF)
        org.apache.log4j.Logger.getLogger("io.netty").setLevel(org.apache.log4j.Level.OFF)
        org.apache.log4j.Logger.getLogger("org.apache.hadoop").setLevel(org.apache.log4j.Level.OFF)

        val conf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
        conf.set("spark.default.parallelism", "2")
        val ssc = new StreamingContext(conf, Seconds(5))
        ssc.addStreamingListener(new MyStreamingListener())
        //    ssc.checkpoint(".")

        val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)

        // 这里没有对生成的rdd数据进行输出操作，所以在web ui上看不到该job
        // webui 上显示的是在executor上执行的job，Driver执行的代码任务不会显示
        // 而且如果batch没有数据的话，任务全都显示skipped
        lines.foreachRDD((rdd, time) => {
            val acc = Tools.getAccInstance(rdd.sparkContext, time)
            if ((time.milliseconds - acc.value) >= 20000) {
                println("rdd 1 =========")
                acc.add(30000)
            }
        })

        val words = lines.flatMap(_.split(","))
        //val wordCounts = words.map(x => (x, 1)).reduceByKeyAndWindow((a: Int, b: Int) => (a + b), Seconds(20), Seconds(10))
        val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

        wordCounts.foreachRDD(rdd => {
//            log.warn("rdd 2 =========")
            rdd.foreachPartition(partition => {
                partition.foreach(record => {
//                    println(record)
                })
            })
        })
        ssc.start()
        ssc.awaitTermination()
    }
}
