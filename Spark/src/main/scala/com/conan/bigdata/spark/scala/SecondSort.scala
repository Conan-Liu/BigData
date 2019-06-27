package com.conan.bigdata.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark 版本的二次排序
  */

class IntPair(val first: Int, val second: Int) extends Ordered[IntPair] with Serializable {
    override def compare(that: IntPair): Int = {
        // 升序
        if (this.first - that.first != 0) {
            this.first - that.first
        } else {
            this.second - that.second
        }
    }
}

object SecondSort {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("SecondSort").setMaster("local[2]")
        val sc = new SparkContext(conf)

        // 初始化每一行数据是两个数字， 总共 10 行
        val data = sc.parallelize(Seq(Seq(10, 2), Seq(3, 3), Seq(4, 2), Seq(2, 2), Seq(3, 1), Seq(3, 20), Seq(-1, 20), Seq(-3, 2), Seq(-1, 2), Seq(-1, 200)), 3)
        // 这里是自定义的 Key， 也可以直接使用Tuple类型
        val pairData = data.map(x => (new IntPair(x(0), x(1)), 0))
        // 直接使用Tuple，实现Tuple的比较
        implicit val tupleSort = new Ordering[Tuple2[Int, Int]] {
            override def compare(x: (Int, Int), y: (Int, Int)): Int = {
                if (x._1 - y._1 != 0) {
                    x._1 - y._1
                } else {
                    x._2 - y._2
                }
            }
        }

        // sortBy(x=>x._1) 底层最终还是转换成 sortByKey((x._1,x))， 增大了数据量， 如果确实不用V， 那就随便写个 0
        val sortedData1 = pairData.sortByKey()
        val sortedData2 = data.map(x => ((x(0), x(1)), 0)).sortByKey()
        // 数据类型一定要注意， 需要显示转才行
        val sortedResult = sortedData1.map(x => (x._1.first, x._1.second.toString)).reduceByKey((x, y) => x + "," + y)
        sortedResult.collect().foreach(println)
        sc.stop()
    }
}