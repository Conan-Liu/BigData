package com.conan.bigdata.flink.scalaapi.transformation

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem

/**
  * 数据倾斜rebalance
  * 数据分区partition
  */
object RebalanceAndPartition {

    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(2)
        val source = env.generateSequence(0, 100)

        // 要多了解考虑Rich为代表的富函数，很多operator都提供了对应富函数来获取任务的相应信息
        // 如果是lambda表达式默认是MapFunction，所以这里必须使用new RichMapFunction来指定相应的map function
        val map1 = source.map(new RichMapFunction[Long, (Long, Long)] {
            override def map(value: Long): (Long, Long) = {
                // 一个subTask对应一个分区，获取当前任务的编号信息，代表分区
                val subtask = getRuntimeContext.getIndexOfThisSubtask
                (subtask, value)
            }
        })


        val sortDS=map1.partitionByRange(0).sortPartition(1, Order.DESCENDING)

        val textSink = sortDS.writeAsText("/Users/mw/temp/rp",FileSystem.WriteMode.OVERWRITE)


        map1.print()

        env.execute("RP")
    }
}
