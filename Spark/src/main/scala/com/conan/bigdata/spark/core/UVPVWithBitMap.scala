package com.conan.bigdata.spark.core

import java.util.Random

import com.conan.bigdata.spark.utils.SparkVariable
import org.roaringbitmap.RoaringBitmap

/**
  * 演示在数据倾斜的情况下计算UV PV
  * 如按地域统计UV PV，显然北上广深的访问量大，其它地方小，造成数据倾斜
  * 数据如下
  * city_id    user_id
  * beijing    11
  * beijing    11
  * beijing    12
  * beijing    11
  * beijing    12
  * shanghai   20
  * shanghai   10
  * shanghai   10
  * xuancheng  9
  * xuancheng  9
  *
  * 1. 可以group by city_id,user_id得到pv,  然后再在等到的结果集上group by city_id计算uv pv，因为pv是可以累加的
  * 2. 也可以给city_id附加随机后缀，打散数据，再根据位图直接计算uv pv
  */
object UVPVWithBitMap extends SparkVariable {

    def main(args: Array[String]): Unit = {
        val sourceRDD = sc.parallelize[(String, Int)](Seq(("beijing", 11), ("beijing", 11), ("beijing", 12), ("beijing", 11), ("beijing", 12), ("shanghai", 20), ("shanghai", 10), ("shanghai", 10), ("xuancheng", 9), ("xuancheng", 9)))

        // 第一种算uv pv方法
        val uvpvRDD1 = sourceRDD.map(x => (x, 1)).reduceByKey(_ + _).map(x => (x._1._1, (1, x._2))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map(x => (x._1, x._2._1, x._2._2))
        println(s"方法1: ${uvpvRDD1.collect().mkString(",")}")

        // 第二种算uv pv方法，这里生产环境要考虑序列化和反序列化
        val uvpvRDD2 = sourceRDD.map(x => (x._1 + "_" + new Random(x._2).nextInt(100), (RoaringBitmap.bitmapOf(x._2), 1))).reduceByKey((x, y) => (RoaringBitmap.or(x._1, y._1), x._2 + y._2))
            .map(x => (x._1.split("_")(0), x._2)).reduceByKey((x, y) => (RoaringBitmap.or(x._1, y._1), x._2 + y._2)).map(x => (x._1, x._2._1.getCardinality, x._2._2))
        println(s"方法2: ${uvpvRDD2.collect().mkString(",")}")
    }
}