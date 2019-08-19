package com.conan.bigdata.spark.scala.example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  */
object NumericOperate {

    def numAvg(sc: SparkContext, nums: RDD[Int]): Unit = {
        // aggregate 方法
        val cnt1 = nums.map(x => (x, 1)).aggregate((0, 0))((x, y) => (x._1 + y._1, x._2 + y._2), (x, y) => (x._1 + y._1, x._2 + y._2))
        println(cnt1._1 / cnt1._2.toDouble)

        // reduce 方法, 为每个元素 x 创建了一个 tuple 对象， 这个效率较低， 考虑 mapPartitions
        val cnt2 = nums.map(x => (x, 1)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
        println(cnt2._1 / cnt2._2.toDouble)

        // mapPartitions 方法
        val cnt3 = nums.mapPartitions(partitions => {
            val list = new ListBuffer[(Int, Int)]()
            var (sum, cnt) = (0, 0)
            partitions.foreach(x => {
                sum += x
                cnt += 1
            })
            // 下面两种返回迭代器都可以
            Iterator((sum, cnt))
            // list.append((sum, cnt))
            // list.toIterator
        }).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
        println(cnt3._1 / cnt3._2.toDouble)
    }

    def showStats(sc: SparkContext, nums: RDD[Int]): Unit = {
        val stats = nums.stats()
        println("总记录数:" + stats.count)
        println("最大值:" + stats.max)
        println("最小值:" + stats.min)
        println("平均值:" + stats.mean)
        println("标准差:" + stats.stdev)
        println("方差:" + stats.variance)
        println("求和:" + stats.sum)
    }

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("NumericOperate").setMaster("local[*]")
        val sc = new SparkContext(sparkConf)
        sc.setLogLevel("WARN")

        val nums = sc.parallelize(Seq(2, 4, 1, 6, 8, 3, 9), 4)

        numAvg(sc, nums)
        println()
        showStats(sc, nums)

    }
}