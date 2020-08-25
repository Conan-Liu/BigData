package com.conan.bigdata.spark.core

import com.conan.bigdata.spark.utils.SparkVariable

/**
  * cache方法把rdd缓存到内存中，其中persist方法提供多种缓存机制
  * checkpoint把rdd持久化存储到本地磁盘或hdfs上
  * 两种方法都能打断依赖链，避免重新从头计算
  */
object CheckpointAndCache extends SparkVariable {

    def main(args: Array[String]): Unit = {
        sc.setCheckpointDir("file:///Users/mw/temp/spark/checkpointexp")

        val data = sc.parallelize[Int](Seq(1, 7, 8, 8, 9, 9, 9, 0, 0))
        println(data.partitions.length)

        val map = data.map(x => {
            println(s"map ==> ${x}")
            (x, "value-" + x)
        }).cache()

        // 注意checkpoint会单独启动一个单独任务从头开始计算，大大的浪费计算资源，所以需要先cache起来
        // 如果注释掉map.cache()方法，则调用action算子时，map函数里println打印一遍，checkpoint方法也会从头计算，重新打印一遍，总共两遍
        // 有map.cache()方法，则执行action算子时，map函数里println打印一遍，紧接着map RDD被cache，然后被checkpoint，不会重新计算map，总共一遍
        map.checkpoint()

        val key = map.map(x => {
            // val a = x._1 / 0
            (x._1, 1)
        }).reduceByKey(_ + _)

        println(map.collect().mkString(","))

        println(key.collect().mkString(","))

    }

}
