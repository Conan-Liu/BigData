package com.conan.bigdata.spark.utils

import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/8/31.
  */
object Spark {

    val HOST_NAME = "CentOS"
    // localhost 和 0.0.0.0 有什么区别， 居然用localhost， 不能接收外部电脑的TCP链接
    val LOCALHOST_NAME = "0.0.0.0"
    val NETCAT_PORT = "44444"


    def getSparkSession(appName: String): SparkSession = {
        val sparkSession = SparkSession.builder().appName(appName).master("local[2]").getOrCreate()
        sparkSession
    }
}