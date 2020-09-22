package com.conan.bigdata.spark.sql

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

/**
  * 强类型的DataSet UDAF
  * 目前还不太理解...
  */
// 中间临时数据类型
case class Average(var sum: Long, var count: Long)

object MyDSAverageUDAF extends Aggregator[Person, Average, Double] {

    // 中间临时的数据初始化
    override def zero: Average = Average(0L, 0L)

    // 聚合操作的合并，可以把数据合并到中间临时数据中，避免创建新的对象
    override def reduce(b: Average, a: Person): Average = {
        b.sum = b.sum + a.age
        b.count = b.count + 1
        b
    }

    // 合并两个中间临时数据
    override def merge(b1: Average, b2: Average): Average = {
        b1.sum = b1.sum + b2.sum
        b1.count = b1.count + b2.count
        b1
    }

    // 输出最终值
    override def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count

    // 指定中间临时数据类型的编码
    override def bufferEncoder: Encoder[Average] = Encoders.product

    // 指定最终输出数据类型的的编码
    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
