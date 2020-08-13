package com.conan.bigdata.spark.core

import com.conan.bigdata.spark.utils.SparkVariable
import org.apache.spark.{HashPartitioner, RangePartitioner}

/**
  * Spark的排序操作
  * 1. 自定义二次排序
  */
object PartitionAndSort extends SparkVariable {

    def main(args: Array[String]): Unit = {

        val isTrue = true
        require(isTrue, "测试require方法")

        val data = sc.parallelize[(Int, Int, String)](Seq((1, 1, "hadoop"), (1, 2, "hive"), (2, 3, "spark"), (2, 4, "flink"), (2, 5, "kafka"), (2, 6, "hbase"), (2, 7, "redis"), (3, 9, "zookeeper"), (3, 8, "xx")), 2)
                .map(x => ((x._1, x._2), x._3))

        // 隐式参数 private val ordering = implicitly[Ordering[K]]，变量名没有意义
        // 对样例类Student的gradeId和sno进行二次排序
        implicit val selfOrder = new Ordering[(Int, Int)] {
            override def compare(x: (Int, Int), y: (Int, Int)): Int = {
                if (x._1 != y._1) {
                    // 第一个排序字段降序
                    if (x._1 > y._1)
                        -1
                    else
                        1
                } else if (x._2 != y._2) {
                    // 第二个排序字段升序
                    if (x._2 > y._2)
                        1
                    else
                        -1
                } else {
                    0
                }
            }
        }

        /**
          * sortByKey()和repartitionAndSortWithinPartitions()的效率差不多，都是ShuffledRDD
          */
        // 只根据 gradeId排序
        val result1 = data.sortByKey()

        // 分区内有序，分区间无序，这里可以通过定义Order对象的隐式参数来升序或降序
        val result2 = data.repartitionAndSortWithinPartitions(new HashPartitioner(3))

        // 使用RangePartitioner实现分区间有序
        val rangePartitioner = new RangePartitioner[(Int, Int), String](data.partitions.length, data)
        val result3 = data.repartitionAndSortWithinPartitions(rangePartitioner)

        println(result1.collect().mkString(","))

        println(result2.collect().mkString(","))

        println(result3.collect().mkString(","))

        sc.stop()
    }
}
