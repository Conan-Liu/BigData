package com.conan.bigdata.spark

import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat
import org.apache.hadoop.io.ArrayWritable
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Hello world!
  */
object App {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("App").setMaster("local[1]")
        val sc = new SparkContext(sparkConf)
        sc.setLogLevel("WARN")
        sc.setCheckpointDir("E:\\checkpoint")

        // 结果并不会顺序打印，因为spark是pipeline执行的，也就是说处理完一条数据，就直接往下继续处理了，不会等到全部处理完再往下
        val source=sc.parallelize[Int](Seq(1,2,3,4,5,6,7,8,9))
        val rdd1=source.map(x=>{
            print(s"${x} ")
            x*2
        })
        // 这条语句只执行一次
        println("正常执行...")
        println()
        val rdd2=rdd1.map(x=>{
            print(s"${x} ")
            x*4
        })
        println(rdd2.toDebugString)
        // 如下有两个action算子，那么这个依赖链上的算子就会执行两次，也就是map算子会执行两遍，如果cache()起来就会打断依赖，map只会执行一遍
        rdd2.cache()
        // hdfs 路径
        // rdd2.checkpoint()
        println(s"求和: ${rdd2.sum()}")
        println(s"最大值: ${rdd2.max()}")
    }
}
