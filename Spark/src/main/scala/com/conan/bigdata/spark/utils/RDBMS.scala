package com.conan.bigdata.spark.utils

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/9/14.
  */
object RDBMS {
    val tableName = "order_search_statistic"

    def main(args: Array[String]): Unit = {
        val sparkSession = SparkSession.builder().appName("RDBMS").getOrCreate()
        val prop = new Properties()
        prop.put("driver", "com.mysql.jdbc.Driver")
        prop.put("user", "pd_test_dev")
        prop.put("password", "VFR5rgdf")
        val order = sparkSession.sqlContext.read.jdbc("jdbc:mysql://10.0.19.6:30242/business_olap?tinyInt1isBit=false&useUnicode=true&characterEncoding=gbk&zeroDateTimeBehavior=convertToNull", "order_search_statistic", prop)

        order.createOrReplaceTempView("aaa")

        sparkSession.sqlContext.sql("select * from aaa").show(false)

    }
}