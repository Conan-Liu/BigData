package com.conan.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2017/1/5.
  */
object FileWithKryoWordCount {
    def main(args: Array[String]) {
        val sparkConf = new SparkConf().setAppName("FileWithKryoWordCount")//.setMaster("local[4]")
        val ssc = new StreamingContext(sparkConf, Seconds(10))
        ssc.sparkContext.setLogLevel("WARN")
        //val lines=ssc.textFileStream("hdfs://192.168.56.101:9000/data/spark")
        // 如果是用本地路径， 记得所有执行节点上都要有一份数据， 不然会报找不到文件
//        val lines = ssc.textFileStream("hdfs://nameservice1/tmp/test")
        val lines = ssc.socketTextStream("nn1.hadoop.pdbd.mwbyd.cn", 9999)
        val words = lines.flatMap(_.split(" "))
        words.persist(StorageLevel.MEMORY_ONLY_SER)
        val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

        //        wordCounts.print()
        wordCounts.count().print()
        ssc.start()
        ssc.awaitTermination()
    }
}
