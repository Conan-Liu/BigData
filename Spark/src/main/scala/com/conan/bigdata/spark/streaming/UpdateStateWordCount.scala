package com.conan.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Conan on 2019/4/27.
  *
  * 该类，记录了从运行时刻， 单词统计的历史记录， 计数， 是从最开始， 一直往后累计的功能
  * 累计的状态checkpoint到了指定目录
  * 状态统计
  */
object UpdateStateWordCount {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setAppName("UpdateStateWordCount").setMaster("local[2]")
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        ssc.sparkContext.setLogLevel("WARN")

        // 使用updateStateByKey 算子， 一定要使用checkpoint保存下来
        // 生产环境上， 建议checkpoint到HDFS上， 这个例子保存到当前路径
        ssc.checkpoint("E:\\Temp\\spark\\UpdateStateWordCount")

        val lines = ssc.socketTextStream("CentOS", 9999)
        val result = lines.flatMap(x => x.split("\\s+")).map(x => (x, 1))

        val state = result.updateStateByKey[Int](updateStateFunc _)
        state.print

        ssc.start
        ssc.awaitTermination
    }


    def updateStateFunc(currentValues: Seq[Int], stateValues: Option[Int]): Option[Int] = {
        val current = currentValues.sum
        val pre = stateValues.getOrElse(0)

        Option(current + pre)
    }
}