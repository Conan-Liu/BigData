package com.conan.bigdata.spark.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

trait StreamingVariable {

    Logger.getLogger("java.lang").setLevel(Level.OFF)
    Logger.getLogger("org.spark_project.jetty").setLevel(Level.WARN)

    // SparkConf的指定必须在SparkContext初始化之前执行
    // 它可以被复制，但是用户不能修改，Spark不支持在程序运行的时候修改参数
    val sparkConf:SparkConf = new SparkConf()
            // 控制台和Yarn Client模式，可以通过Ctrl-C来传递kill信号，触发钩子方法执行优雅的关闭，Yarn Cluster不太好传递kill信号，使用监听信号的方式触发
            // .set("spark.streaming.stopGracefullyOnShutdown", "true")
    if ("".equals(sparkConf.get("spark.app.name", "")))
        sparkConf.setAppName(getClass.getName)
    if ("".equals(sparkConf.get("spark.master", "")))
        sparkConf.setMaster("local[4]")
    val sc:SparkContext = SparkContext.getOrCreate(sparkConf)

    sc.setLogLevel("WARN")

    val ssc = new StreamingContext(sc, Seconds(10))
    // ssc.sparkContext.setLogLevel("WARN")
}
