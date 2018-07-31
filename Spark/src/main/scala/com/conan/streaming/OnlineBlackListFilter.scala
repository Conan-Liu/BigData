package com.conan.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * Created by Administrator on 2017/1/4.
  */
object OnlineBlackListFilter {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("local").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Durations.seconds(10))
    val blackList = Array(("hadoop", true), ("mahout", true))
    val blackListRDD=ssc.sparkContext.parallelize(blackList,8)
    blackListRDD.cache()
    val clickStream=ssc.socketTextStream("master",9999)

    val clickStreamFormatted=clickStream.map(ads=>(ads.split("\\s+")(1),ads))
    clickStreamFormatted.transform(clickRDD=>{
      val joinedBlackListRDD=clickRDD.leftOuterJoin(blackListRDD)
      val validClicked=joinedBlackListRDD.filter(joinedItem=>{
        if(joinedItem._2._2.getOrElse(false))
          false
        else
          true
      })
      validClicked.map(validClicked=>{validClicked._2._1})
    }).print()

    ssc.start()
    ssc.awaitTermination()
  }
}


//Time: 1483498200000 ms
//Time: 1483498500000 ms
//Time: 1483498800000 ms
//Time: 1483499100000 ms