package com.conan.bigdata.spark.sparksql

import java.util.Properties

import com.conan.bigdata.spark.utils.Spark
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/12/17.
  */
object jdbc {
    val URL = "jdbc:mysql://10.0.19.6:30242/test?tinyInt1isBit=false&useUnicode=true&characterEncoding=gbk&zeroDateTimeBehavior=convertToNull"
    val TABLE_NAME = "aaa"

    def main(args: Array[String]): Unit = {
        val properties = new Properties()
        properties.put("driver", "com.mysql.jdbc.Driver")
        properties.put("user", "pd_test_dev")
        properties.put("password", "VFR5rgdf")
        val sparkSession = Spark.getSparkSession("jdbc")
        val jdbcDF = sparkSession.sqlContext.read.jdbc(URL, TABLE_NAME, properties)
        jdbcDF.show()
    }
}