package com.conan.bigdata.spark.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Scala Trait(特征) 相当于 Java 的接口，可以被多继承
  * 实际上它比接口还功能强大，与接口不同的是，它还可以定义属性和方法的实现，更像抽象类，却又比类可以多继承，有点强
  * 用关键字extend继承
  *
  * 可参考[[com.conan.bigdata.spark.sql.project.Processor]]
  */
trait SparkVariable {

    // Spark 使用org.apache.log4j来记录日志
    private val log: Logger = Logger.getLogger("SparkVariable")

    // 这个放前面写，SparkContext SparkSession初始化会有很多日志，提前关闭
    // Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("java.lang").setLevel(Level.OFF)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)
    Logger.getLogger("org.spark_project.jetty").setLevel(Level.WARN)

    // 该方法为了解决刚启动executor不知道数据位置的问题，提高数据本地行
    // InputFormatInfo.computePreferredLocations()

    // SparkConf的指定必须在SparkContext初始化之前执行
    // 它可以被复制，但是用户不能修改，Spark不支持在程序运行的时候修改参数
    val sparkConf = new SparkConf()
    if ("".equals(sparkConf.get("spark.app.name", "")))
        sparkConf.setAppName(getClass.getName)
    if ("".equals(sparkConf.get("spark.master", "")))
        sparkConf.setMaster("local[2]")
    val sc:SparkContext = SparkContext.getOrCreate(sparkConf)
    sc.setLogLevel("WARN")
    // 这里SparkSession的初始化会使用之前定义的SparkContext，不会再重新创建，也就是会先判断上下文有没有SparkContext
    val spark:SparkSession = SparkSession.builder().getOrCreate()

}