package com.conan.bigdata.spark.sparksql

import com.conan.bigdata.spark.utils.SparkVariable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object ReadJsonAndParquet extends SparkVariable {
    val PARQUET_PATH = ""

    def readJson(spark: SparkSession): Unit = {

        val json = "{\"name\":\"liu\",\"age\":100,\"hobby\":{\"f1\":\"666\",\"f2\":\"厉害厉害\",\"f3\":\"牛逼牛逼\"}}"
        val schema = StructType(
            List(
                StructField("name", StringType, true),
                StructField("liu", IntegerType, true),
                StructField("hobby", StringType, true),
                StructField("create_time", StringType, true)
            )
        )
        println("json DataSet...")
        import spark.implicits._
        val jsonDS = spark.createDataset[String](Seq(json))
        jsonDS.show(false)

        println("json DataFrame...")
        val jsonDF = spark.read.schema(schema).json(jsonDS)
        jsonDF.show(false)

        // val jsonDFPath=spark.read.json("/repository/parquet/user_action_wechat_operation/2018-09-01")
        // jsonDFPath.write.json("/user/hdfs/json/user_action_wechat_operation/2018-09-01")
    }

    def readParquet(spark: SparkSession): Unit = {
        // 这个read方法， 默认是从hdfs读取数据， 如果需要读本地的，需要加前缀file://
        val parquetDF = spark.read.parquet("/repository/parquet/user_action_wechat_operation/2018-09-01")
        val textDF = spark.read.text("/repository/kafka/user_biz_user_tag/2018-12-18")
        parquetDF.write.json("/user/hdfs/json/user_action_wechat_operation/2018-09-01")
        parquetDF.createOrReplaceTempView("tab1")
        parquetDF.printSchema()
        spark.sql("select user_id from tab1").show(1000, false)
    }

    def main(args: Array[String]): Unit = {

        readJson(spark)
    }
}