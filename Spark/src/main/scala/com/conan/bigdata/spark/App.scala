package com.conan.bigdata.spark

import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat
import org.apache.hadoop.io.ArrayWritable
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Hello world!
  *
  */
object App {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("UserTagDetailHBase")
        val sc = new SparkContext(sparkConf)

        val sss = sc.hadoopFile("/user/deploy/mr/out/part-r-00000.parquet", classOf[MapredParquetInputFormat], classOf[Void], classOf[ArrayWritable])

        val df = sss.map(x => {
            val value = x._2.get()
            (String.valueOf(value(0)), String.valueOf(value(1)))
        })

        df.foreach(println)
    }
}
