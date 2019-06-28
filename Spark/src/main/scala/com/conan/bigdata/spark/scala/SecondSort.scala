package com.conan.bigdata.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark 版本的二次排序
  * 数据如下，
  * aa 11
  * bb 11
  * cc 34
  * aa 22
  * bb 67
  * cc 29
  * aa 36
  * bb 33
  * cc 30
  * aa 42
  * bb 44
  * cc 49
  *
  * 需求：
  * 1、对上述数据按key值进行分组
  * 2、对分组后的值进行排序
  * 3、截取分组后值得top 3位以key-value形式返回结果
  *
  * 可以使用二次排序， 如果Key比较均匀， 可以直接groupbyKey， 然后mapValue
  */

class SecondSortKey(val first: String, val second: Int) extends Ordered[SecondSortKey] with Serializable {
    override def compare(that: SecondSortKey): Int = {
        // 升序
        if (this.first.compareTo(that.first) != 0) {
            this.first.compareTo(that.first)
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
        val data = sc.parallelize(Seq(("aa", 11), ("bb", 11), ("cc", 34), ("aa", 22), ("bb", 67), ("cc", 29), ("aa", 36), ("bb", 33), ("cc", 30), ("aa", 42), ("bb", 44), ("cc", 49)), 3)
        // 这里是自定义的 Key， 也可以直接使用Tuple类型
        val pairData = data.map(x => (new SecondSortKey(x._1, x._2), 0))
        // 直接使用Tuple，实现Tuple的比较
        implicit val tupleSort = new Ordering[Tuple2[String, Int]] {
            override def compare(x: (String, Int), y: (String, Int)): Int = {
                if (x._1.compareTo(y._1) != 0) {
                    x._1.compareTo(y._1)
                } else {
                    x._2 - y._2
                }
            }
        }

        // sortBy(x=>x._1) 底层最终还是转换成 sortByKey((x._1,x))， 增大了数据量， 如果确实不用V， 那就随便写个 0
        val sortedData1 = pairData.sortByKey()
        val sortedData2 = data.map(x => ((x._1, x._2), 0)).sortByKey()
        // 数据类型一定要注意， 需要显示转才行
        val sortedResult = sortedData1.map(x => (x._1.first, x._1.second.toString)).reduceByKey((x, y) => x + "," + y)

        // 直接通过groupByKey来计算, 直接转List，然后排序反转
        val groupRDD = data.groupByKey().mapValues(x => x.toList.sorted.reverse.take(3))
        // 如果多个action的，每个action会从头开始计算， 这种比较操作， 可以cache()打断依赖链
        sortedResult.collect().foreach(println)
        println("==============================")
        groupRDD.collect().foreach(println)
        sc.stop()
    }
}