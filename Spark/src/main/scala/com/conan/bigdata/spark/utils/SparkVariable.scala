package com.conan.bigdata.spark.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  */
trait SparkVariable {

    // SparkConf的指定必须在SparkContext初始化之前执行
    val sparkConf = new SparkConf().setAppName(getClass.getName)
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession.builder().getOrCreate()
}