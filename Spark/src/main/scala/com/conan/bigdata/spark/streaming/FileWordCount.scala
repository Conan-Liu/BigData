package com.conan.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2017/1/5.
  */
object FileWordCount {
    def main(args: Array[String]) {
        val sparkConf = new SparkConf().setAppName("FileWordCount").setMaster("local[1]")
        val ssc = new StreamingContext(sparkConf, Seconds(10))
        ssc.sparkContext.setLogLevel("WARN")
        //val lines=ssc.textFileStream("hdfs://192.168.56.101:9000/data/spark")
        val lines = ssc.textFileStream("F:\\BigData\\Spark\\aa")
        val words = lines.flatMap(_.split(" "))
        val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

        wordCounts.print()
        ssc.start()
        ssc.awaitTermination()
    }
}
