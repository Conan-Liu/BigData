package com.conan.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Conan on 2019/5/23.
  */
object WindowWordCount {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("UpdateStateWordCount").setMaster("local[2]")
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        ssc.sparkContext.setLogLevel("WARN")

        val lines = ssc.socketTextStream("CentOS", 9999)
        lines.persist(StorageLevel.MEMORY_ONLY_SER)
//        val result1 = lines.count()
        // 这里滑动窗口是 10s， 2倍的批次， 所以， 执行的时候， 是按照2倍的批次时间执行一次任务， 也就是10秒执行一次
        // 这样的话，等于每个批次的RDD合并成一个RDD来处理
        // 这里是 10 秒执行一次任务， 不再是定义的 5秒
        val result2 = lines.window(Seconds(30), Seconds(10)).count()
        //        val result = lines.window(Seconds(15), Seconds(10)).flatMap(x => x.split("\\s+")).map(x => (x, 1)).reduceByKey(_ + _)

//        result1.print()
        result2.print()

        ssc.start()
        ssc.awaitTermination()
    }
}