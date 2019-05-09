package com.conan.bigdata.spark.streaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.util.LongAccumulator

/**
  * Created by Conan on 2019/5/9.
  */
object AccumulatorsBroadcastWordCount {

    @volatile private var broadcast: Broadcast[Seq[String]] = null
    @volatile private var longAccumulator: LongAccumulator = null

    def getBroadcastInstance(sc: SparkContext): Broadcast[Seq[String]] = {
        if (broadcast == null) {
            synchronized {
                if (broadcast == null) {
                    val wordBlackList = Seq("a", "b", "c")
                    broadcast = sc.broadcast[Seq[String]](wordBlackList)
                }
            }
        }
        broadcast
    }

    def getLongAccumulatorInstance(sc: SparkContext): LongAccumulator = {
        if (longAccumulator == null) {
            synchronized {
                if (longAccumulator == null) {
                    longAccumulator = sc.longAccumulator("WordsInBlacklistCounter")
                }
            }
        }
        longAccumulator
    }

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("AccumulatorsBroadcastWordCount").setMaster("local[2]")
        val ssc = new StreamingContext(conf, Seconds(5))
        ssc.sparkContext.setLogLevel("WARN")

        val lines = ssc.socketTextStream("CentOS", 9999, StorageLevel.MEMORY_AND_DISK_SER)
        // val wordsCnt = lines.flatMap(_.split("\\s+")).map(x => (x, 1)).reduceByKey(_ + _)
        val wordsCnt = lines.flatMap(_.split("\\s+"))

        wordsCnt.foreachRDD((rdd: RDD[(String)], time: Time) => {
            // 获取黑名单广播变量， 用于数据量比较小，才广播
            val blackList = AccumulatorsBroadcastWordCount.getBroadcastInstance(rdd.sparkContext)
            // 定义一个全局Long型累加器， 记录被滤掉的黑名单记录数
            val dropWordCount = AccumulatorsBroadcastWordCount.getLongAccumulatorInstance(rdd.sparkContext)

            val counts = rdd.filter(x => {
                if (blackList.value.contains(x)) {
                    dropWordCount.add(1)
                    false
                } else {
                    true
                }
            }).collect().mkString("[", ", ", "]")

            println(s"${time} : ${counts}, 共滤掉黑名单记录数 : ${dropWordCount.value}")
        })
        //        wordsCnt.print()
        ssc.start()
        ssc.awaitTermination()
    }
}