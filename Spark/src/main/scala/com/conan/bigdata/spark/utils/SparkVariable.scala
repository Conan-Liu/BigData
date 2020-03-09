package com.conan.bigdata.spark.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

trait SparkVariable {

    // 这个放前面写，SparkContext SparkSession初始化会有很多日志，提前关闭
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("java.lang").setLevel(Level.OFF)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)
    Logger.getLogger("org.spark_project.jetty").setLevel(Level.WARN)

    // SparkConf的指定必须在SparkContext初始化之前执行
    // 它可以被复制，但是用户不能修改，Spark不支持在程序运行的时候修改参数
    val sparkConf = new SparkConf()
    if ("".equals(sparkConf.get("spark.app.name","")))
        sparkConf.setAppName(getClass.getName)
    if ("".equals(sparkConf.get("spark.master","")))
        sparkConf.setMaster("local[*]")
    val sc = SparkContext.getOrCreate(sparkConf)
    // sc.setLogLevel("WARN")
    // 这里SparkSession的初始化会使用之前定义的SparkContext，不会再重新创建，也就是会先判断上下文有没有SparkContext
    val spark = SparkSession.builder().getOrCreate()

}