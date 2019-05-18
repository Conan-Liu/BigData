package com.conan.bigdata.spark.scala.example

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2019/5/18.
  */
object CalcAvg {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("CalcAvg").setMaster("local[*]")
        val sc = new SparkContext(sparkConf)

        val nums = sc.parallelize(Seq(2, 4, 1, 6, 8, 3, 9), 4)
        // aggregate 方法
        val cnt1 = nums.map(x => (x, 1)).aggregate((0, 0))((x, y) => (x._1 + y._1, x._2 + y._2), (x, y) => (x._1 + y._1, x._2 + y._2))
        println(cnt1._1 / cnt1._2.toDouble)

        // reduce 方法, 为每个元素 x 创建了一个 tuple 对象， 这个效率较低， 考虑 mapPartitions
        val cnt2 = nums.map(x => (x, 1)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
        println(cnt2._1 / cnt2._2.toDouble)

        // mapPartitions 方法
        val cnt3 = nums.mapPartitions(partitions => {
            var (sum, cnt) = (0, 0)
            partitions.map(x => {
                sum += x
                cnt += 1
            })
            (sum,cnt)
        })

    }
}