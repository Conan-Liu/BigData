package com.conan.bigdata.spark.sparksql

import com.conan.bigdata.spark.utils.SparkVariable

/**
  */
object ReadParquetWithPath extends SparkVariable{
    val PARQUET_PATH = ""

    def main(args: Array[String]): Unit = {
        
        // 这个read方法， 默认是从hdfs读取数据， 如果需要读本地的，需要加前缀file://
        val parquetDF = spark.read.parquet("/repository/parquet/user_action_wechat_operation/2018-09-01")
        val textDF=spark.read.text("/repository/kafka/user_biz_user_tag/2018-12-18")
        parquetDF.write.json("/user/hdfs/json/user_action_wechat_operation/2018-09-01")

        val jsonDF=spark.read.json("/repository/parquet/user_action_wechat_operation/2018-09-01")
        jsonDF.write.json("/user/hdfs/json/user_action_wechat_operation/2018-09-01")

        parquetDF.createOrReplaceTempView("tab1")
        parquetDF.printSchema()
        //        spark.sqlContext.sql("select user_id from tab1").show(1000, false)
    }
}