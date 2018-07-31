package com.conan.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2017/1/5.
  */
object NetworkWordCount {
  def main(args: Array[String]) {
    //设置日志输出级别，省的控制台全是没用的日志
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("java.lang").setLevel(Level.OFF)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint(".")

    val lines = ssc.socketTextStream("master", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    val words = lines.flatMap(_.split(","))
    val wordCounts = words.map(x => (x, 1)).reduceByKeyAndWindow((a: Int, b: Int) => (a + b), Seconds(20), Seconds(10))

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
