package com.conan.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2017/1/5.
  */
object FileWordCount {
  def main(args: Array[String]) {
    val sparkConf=new SparkConf().setAppName("FileWordCount").setMaster("local[2]")
    val ssc=new StreamingContext(sparkConf,Seconds(20))
    //val lines=ssc.textFileStream("hdfs://192.168.56.101:9000/data/spark")
    val lines=ssc.textFileStream("E:/Spark/test_log")
    val words=lines.flatMap(_.split(" "))
    val wordCounts=words.map(x=>(x,1)).reduceByKey(_+_)

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
