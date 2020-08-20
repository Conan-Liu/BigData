package com.conan.bigdata.spark.structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

/**
  * Structured Streaming 实现的WordCount
  */
object WordCount extends StructuredVariable{

    def main(args: Array[String]): Unit = {
        // 初始化环境
        spark.sparkContext.setLogLevel("warn")
        import spark.implicits._

        if(spark ne null){
            println("spark is not null ...")
        }

        // 定义输入流
        val lines=spark.readStream.format("socket").option("host","localhost").option("port",9999).load()

        // transform
        val words = lines.as[String].flatMap(_.split(" "))

        // result table
        val wordCounts=words.groupBy("value").count()

        // 触发active执行程序
        val query=wordCounts.writeStream.outputMode(OutputMode.Append()).format("console").start()

        // 阻止退出
        query.awaitTermination()
    }
}
