package com.conan.bigdata.spark.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  */
trait SparkVariable {

    // SparkConf的指定必须在SparkContext初始化之前执行
    val sparkConf = new SparkConf().setAppName(getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession.builder().getOrCreate()

    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("java.lang").setLevel(Level.OFF)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)
}