package com.conan.bigdata.spark.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * checkpoint的作用
  * 1. Driver能失败后从之前的上下文恢复过来，目的还是为了exactly once准确一次消费
  * 2. 数据不丢失，分为两种情况，
  * 对于第一种作用
  *   代码修改后，重启时会使用已保存的老的序列化文件重新生成
  *   这里可能会反序列化失败，或者反序列化的内容还是老代码
  * 对于第二种作用
  * (1) 不带状态的算子，可以直接从保存的offset来重新计算，输出保持幂等性，没必要使用checkpoint
  * (2) 带状态的算子，如window算子
  */
object CheckpointExp {

    //设置日志输出级别，省的控制台全是没用的日志
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("java.lang").setLevel(Level.OFF)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)

    /**
      * 业务逻辑必须要写在这个创建StreamingContext的方法里，否则会报如下错
      * org.apache.spark.SparkException: org.apache.spark.streaming.dstream.XXXXXXXXDStream@38bc5ac0 has not been initialized
      */
    def createStreamingContext(ip: String, port: String, checkpointDir: String): StreamingContext = {
        println("create new context")
        val batchDuration = Seconds(5)
        val conf = new SparkConf().setAppName("NetworkWordCount")
        val ssc = new StreamingContext(conf, batchDuration)
        ssc.checkpoint(checkpointDir)

        val lines = ssc.socketTextStream(ip, port.toInt, StorageLevel.MEMORY_ONLY_SER)

        val words = lines.flatMap(_.split("\\s+")).map((_, 1))
        // words.checkpoint(batchDuration * 10)
        // 这里batch是5s，滑动窗口是10s，计算的时候是按照滑动窗口来计时，也就是说每两个batch，计算一次，时间跨度是向前40s，8个batch
        // val wordCounts = words.map(x => (x, 1)).reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(1800), Seconds(20), 2)
        val wordCounts = words.window(batchDuration * 360, batchDuration * 4).reduceByKey(_ + _)
        wordCounts.print()
        val wordCnt=wordCounts.count()
        wordCnt.print()
        ssc
    }

    /**
      * main 方法不要写逻辑，否则反序列化有问题，如下是标准代码套路
      */
    def main(args: Array[String]) {

        val Array(ip, port, checkpointDir) = Array[String]("192.168.1.8", "9999", "/user/root/temp/spark/checkpoint/checkpointexp")

        /**
          * () => createStreamingContext(checkpointDir) 表示这是一个无参的函数，返回值就是createStreamingContext方法的返回值
          * 实现Driver失败后能从checkpoint中恢复，失败前后是同一个Context
          * 第一步
          * 当应用程序第一次启动的时候，需要创建一个StreamingContext,并且调用其start方法进行启动
          * 当Driver从失败中恢复过来时，需要从checkpoint目录记录的元数据中恢复出来一个StreamingContext
          * 第二步
          * 能监控Driver运行状况，并在失败的时候，重新拉起Driver进程，yarn的RM可以实现
          *
          * 注意：就算不这么写，yarn也会在driver挂了的时候拉起，不过意味着这会重新创建一个Context，和失败前的Context不一样
          * 而且，如果达到了yarn的重试次数值，那么再失败的话，任务就真的失败了
          */
        val ssc = StreamingContext.getOrCreate(checkpointDir, () => createStreamingContext(ip, port, checkpointDir))

        ssc.start()
        ssc.awaitTermination()
    }
}