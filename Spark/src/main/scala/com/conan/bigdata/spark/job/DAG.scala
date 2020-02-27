package com.conan.bigdata.spark.job

import com.conan.bigdata.spark.utils.SparkVariable

import scala.collection.mutable.ListBuffer

/**
  * 验证DAG执行情况
  *
  * 从代码的action算子，根据血缘关系向上追溯，按路径得到一个DAG
  *
  * 每个action算子对应一个DAG，也就是一个job，一个Application可以有多个action算子
  * 一个DAG内数据Pipleline执行，和代码先后顺序无关
  * 多个DAG之间，顺序执行
  *
  * 可以看成DAG图就是一个管道，数据进来了以后沿着管道顺序往下流，代码的逻辑定义了管道，和写代码的顺序无关
  */
object DAG extends SparkVariable {

    def main(args: Array[String]): Unit = {

        val source = sc.parallelize[String](Seq("a", "b", "c", "d", "e", "f", "g", "h"), 2)

        val action1_1 = source.mapPartitions(x => x.map(i => {
            println(s"action1_1: ${i}")
            (i, 1)
        }), true)
        val action1_2 = source.map((_, 0 to 10)).flatMap(x => {
            println(s"action1_2: ${x._1}")
            val list = new ListBuffer[(String, Int)]()
            for (i <- x._2) {
                list.append((x._1, i))
            }
            list
        })
        val action1_3 = action1_1.union(action1_2).reduceByKey(_ + _)
        // 这是一个从sourceRDD来的action算子，对应一个DAG
        println(s"action1的结果: ${action1_3.map(_._1).collect().mkString(",")}")

        // 这是基于 action1_3RDD来的action算子，对应一个DAG，println是action算子
        // 注意，查看web界面，union步骤skip了，直接从reduceByKey执行，说明前面的依赖没执行，直接利用现成数据，
        // union 会打断依赖？
        println(s"action1的总和: ${action1_3.map(_._2).sum()}")
        // 这个是从sourceRDD计算而来，对应一个DAG，可以看打印结果，action1_2里面的println执行了两遍
        println(s"action1_2的总和: ${action1_2.count()}")

        // // 这是一个从sourceRDD来的action算子，对应一个DAG
        val action2_1 = source.map(_.concat("-1"))
        println(s"action1的结果: ${action2_1.collect().mkString(",")}")

        Thread.sleep(1000000)
        sc.stop()
    }
}