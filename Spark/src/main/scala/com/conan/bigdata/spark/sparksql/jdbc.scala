package com.conan.bigdata.spark.sparksql

import java.util.Properties

import com.conan.bigdata.spark.utils.SparkVariable

/**
  */
object jdbc extends SparkVariable{
    val URL = "jdbc:mysql://10.0.19.6:30242/test?tinyInt1isBit=false&useUnicode=true&characterEncoding=gbk&zeroDateTimeBehavior=convertToNull"
    val TABLE_NAME = "aaa"

    def main(args: Array[String]): Unit = {
        val properties = new Properties()
        properties.put("driver", "com.mysql.jdbc.Driver")
        properties.put("user", "pd_test_dev")
        properties.put("password", "VFR5rgdf")
        val jdbcDF = spark.sqlContext.read.jdbc(URL, TABLE_NAME, properties)
        jdbcDF.show()
    }
}