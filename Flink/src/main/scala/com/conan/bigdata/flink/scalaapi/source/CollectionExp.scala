package com.conan.bigdata.flink.scalaapi.source

import org.apache.flink.api.scala._

/**
  * DataSource主要两类
  * 1. 基于集合
  * 2. 基于文件
  * 该类时基于集合的演示
  */
object CollectionExp {

    def main(args: Array[String]): Unit = {
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

        // 传入一个可变参数，内部调用fromCollection
        val eleDS: DataSet[String] = env.fromElements[String]("spark", "hadoop", "flink")
        val collectionDS:DataSet[String] = env.fromCollection[String](Array("spark","hadoop","flink","array"))

        val sequenceDS:DataSet[Long] = env.generateSequence(1,20)

        // 标准输出
        eleDS.print()
        // 错误输出
        eleDS.printToErr()
        collectionDS.print()
        sequenceDS.print()
    }
}
