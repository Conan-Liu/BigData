package com.conan.bigdata.spark.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  */
object OnlineBlackListFilter1 {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("OnlineBlackListFilter1").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Durations.seconds(10))
    val sc = ssc.sparkContext
    val black_list = sc.parallelize(Array("fail", "sad")).map(black_word => (black_word, black_word))

    sc.setCheckpointDir("E:\\Spark\\")

    val input_word = ssc.socketTextStream("master", 9999)
    val flattenWord = input_word.flatMap(_.split("\\s+")).map(row => (row, row))

    val not_black_word = flattenWord.transform(fw => {
      fw.leftOuterJoin(black_list).filter(_._2._2.isEmpty).map(_._1)
    })


    not_black_word.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
