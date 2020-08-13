package com.conan.bigdata.spark.core

import com.conan.bigdata.spark.utils.SparkVariable

/**
  * RDD Checkpoint演示
  */
object CheckpointExp extends SparkVariable {

    def main(args: Array[String]): Unit = {
        sc.setCheckpointDir("file:///Users/mw/temp/spark/checkpointexp")

        val data = sc.parallelize[Int](Seq(1, 7, 8, 8, 9, 9, 9, 0, 0))
        println(data.partitions.length)

        val map = data.map(x => {
            println(s"map ==> ${x}")
            (x, "value-" + x)
        })

        // 注意checkpoint会单独启动一个单独任务从头开始计算，大大的浪费计算资源，所以需要先cache起来
        map.cache()
        map.checkpoint()

        val key = map.map(x => {
            // val a = x._1 / 0
            (x._1, 1)
        }).reduceByKey(_ + _)

        println(map.collect().mkString(","))

        println(key.collect().mkString(","))

    }

}
