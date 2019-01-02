package com.conan.bigdata.spark.sparksql

import com.conan.bigdata.spark.utils.Spark

/**
  * Created by Administrator on 2018/9/26.
  */
object ReadParquetWithPath {
    val PARQUET_PATH = ""

    def main(args: Array[String]): Unit = {
        val sparkSession = Spark.getSparkSession("ReadParquetWithPath")
        val parquetDF = sparkSession.read.parquet("/repository/parquet/user_action_wechat_operation/2018-09-01")
        val textDF=sparkSession.read.schema().text("/repository/kafka/user_biz_user_tag/2018-12-18")
        parquetDF.write.json("/user/hdfs/json/user_action_wechat_operation/2018-09-01")

        val jsonDF=sparkSession.read.json("/repository/parquet/user_action_wechat_operation/2018-09-01")
        jsonDF.write.json("/user/hdfs/json/user_action_wechat_operation/2018-09-01")

        parquetDF.createOrReplaceTempView("tab1")
        parquetDF.printSchema()
        //        sparkSession.sqlContext.sql("select user_id from tab1").show(1000, false)
    }
}