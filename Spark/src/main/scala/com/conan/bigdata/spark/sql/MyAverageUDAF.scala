package com.conan.bigdata.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  */
object MyAverageUDAF extends UserDefinedAggregateFunction {

    // 输入数据的类型
    def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)

    // 定义中间聚合结果的数据类型
    override def bufferSchema: StructType = StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)

    // 输出结果数据的类型
    override def dataType: DataType = DoubleType

    // 确定性，是否在相同的输入时，输出相同的结果
    override def deterministic: Boolean = true

    // 中间聚合数据的初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = 0L
        buffer(1) = 0L

        // 可以使用update方法
        // buffer.update(0,0)
    }

    // 根据输入数据来更新中间聚合数据
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        if(!input.isNullAt(0)) {
            buffer(0) = buffer.getLong(0) + input.getLong(0)
            buffer(1) = buffer.getLong(1) + 1
        }
    }

    // 合并两个中间聚合的结果，存入第一个结果，因为任务是分布式执行的，所以需要对中间结果合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
        buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    override def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)
}
