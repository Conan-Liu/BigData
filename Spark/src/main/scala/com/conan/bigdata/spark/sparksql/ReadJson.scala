package com.conan.bigdata.spark.sparksql

import com.conan.bigdata.spark.utils.SparkVariable
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * json 格式
  * {"name":"liu","age":100,"hobby":{"f1":"666","f2":"厉害厉害","f3":"牛逼牛逼"}}
  */
object ReadJson extends SparkVariable{
    def main(args: Array[String]): Unit = {
        val schema = StructType(
            List(
                StructField("mobile", StringType, true),
                StructField("device_id", StringType, true),
                StructField("device_type", StringType, true),
                StructField("create_time", StringType, true)
            )
        )
        // 指定json文件， json文件中的每条json格式如上，否则不能解析
        //        val sparkDF = spark.read.schema(schema).json("/repository/kafka/basic_app_trace/*")

        // 直接指定数据样例， 用于测试
        import spark.implicits._
        val jsonStr = "{\"name\":\"liu\",\"age\":100,\"hobby\":{\"f1\":\"666\",\"f2\":\"厉害厉害\",\"f3\":\"牛逼牛逼\"}}"
        val jsonDS = spark.createDataset[String](Array(jsonStr).toSeq)
        val jsonDF = spark.read.schema(schema).json(jsonDS)
        jsonDF.printSchema()
        jsonDF.show(false)
    }
}