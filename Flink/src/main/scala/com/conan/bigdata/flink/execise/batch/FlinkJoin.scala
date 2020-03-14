package com.conan.bigdata.flink.execise.batch

import org.apache.flink.api.scala._

/**
  * 演示 Flink 的join算子
  * 比Spark PairRDD的关联灵活很多
  * 数据一
  * name    city_id    age
  * liu      100       100
  * liu      99        18
  * li       98        55
  *
  * 数据二
  * pv     name    city_id
  * 10     liu     100
  * 12     liu     99
  * 9      liu     99
  * 8      li      98
  */
object FlinkJoin {

    def main(args: Array[String]): Unit = {
        val data1 = Seq(("liu", 100, 100), ("liu", 99, 18), ("li", 98, 55))
        val data2 = Seq((10, "liu", 100), (12, "liu", 99), (9, "liu", 99), (8, "li", 98))

        val env = ExecutionEnvironment.getExecutionEnvironment
        val dataSet1 = env.fromCollection(data1)
        val dataSet2 = env.fromCollection(data2)

        // 可以一个字段做关联， 这个字段可以指定位置，不是必需第一列
        val join1 = dataSet1.join(dataSet2).where(0).equalTo(1).apply((x, y) => (x._1, x._2, x._3, y._1))
        join1.print()

        println("***********************************************************************")
        // 可以多个字段做关联， 关联字段顺序要对上
        val join2 = dataSet1.join(dataSet2).where(0, 1).equalTo(1, 2).apply((x, y) => (x._1, x._2, x._3, y._1))
        join2.print()
    }
}