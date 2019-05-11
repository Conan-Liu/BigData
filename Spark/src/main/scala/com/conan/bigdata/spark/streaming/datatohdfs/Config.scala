package com.conan.bigdata.spark.streaming.datatohdfs

import org.apache.spark.SparkConf

/**
  * Created by Conan on 2019/5/11.
  */
object Config {

    def getSparkConf(sparkConf: SparkConf): Map[String, String] = {
        var map: Map[String, String] = Map()
        sparkConf.getAll.foreach(x => {
            map += (x._1 -> x._2)
        })
        map
    }
}