package com.conan.bigdata.spark.utils

import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/8/31.
  */
object Spark {
    def getSparkSession(appName:String):SparkSession={
        val sparkSession=SparkSession.builder().appName(appName).getOrCreate()
        sparkSession
    }
}