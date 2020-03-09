package com.conan.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

object SparkSqlWordCount {

    case class Word(word: String, cnt: Int)

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("SparkSqlWordCount").setMaster("local[2]")
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        ssc.sparkContext.setLogLevel("WARN")

        val lines = ssc.socketTextStream("CentOS", 9999)
        val result = lines.flatMap(x => x.split("\\s+")).map(x => (x, 2))

        // 注意这里RDD的类型， 对应上面map输出的元组
        // 这里的 Time 类就是为了获取系统执行时间
        result.foreachRDD((rdd: RDD[(String, Int)], time: Time) => {
            val sparkSession = SparkSession.builder().config(ssc.sparkContext.getConf).getOrCreate()
            // 调用隐式转换实现RDD转成DataFrame
            import sparkSession.implicits._
            val wordDF = rdd.map(x => Word(x._1, x._2)).toDF()
            wordDF.createOrReplaceTempView("word")
            val sqlDF = sparkSession.sql("select word, sum(cnt) as cnt from word group by word")
            println(s"============$time========================================")
            sqlDF.show()
        })

        ssc.start()
        ssc.awaitTermination()
    }
}