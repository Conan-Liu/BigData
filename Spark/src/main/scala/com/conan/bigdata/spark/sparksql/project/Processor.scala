package com.conan.bigdata.spark.sparksql.project

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

trait Processor {

    private val log = LoggerFactory.getLogger("Processor")
    Logger.getLogger("org.spark_project.jetty").setLevel(Level.WARN)

    val sparkConf = new SparkConf()
    if ("".equals(sparkConf.get("spark.app.name", "")))
        sparkConf.setAppName(getClass.getSimpleName)
    if ("".equals(sparkConf.get("spark.master", "")))
        sparkConf.setMaster("local[2]")
    val sc: SparkContext = SparkContext.getOrCreate(sparkConf)
    // sc.setLogLevel("WARN")
    // 这里SparkSession的初始化会使用之前定义的SparkContext，不会再重新创建，也就是会先判断上下文有没有SparkContext
    val spark: SparkSession = SparkSession.builder().getOrCreate()

    // 重置数据
    def reset(targetDate: String): Unit

    // 具体计算逻辑的抽象方法
    def execute(targetDate: String): Unit

    // 普通方法
    def close(): Unit = {
        println("close")
    }
}
