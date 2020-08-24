package com.conan.bigdata.flink.scalaapi.transformation

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem

/**
  *
  */
object Accumulator {

    def main(args: Array[String]): Unit = {

        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        // 学生数据集，广播
        val words = env.fromCollection(List[String]("aaa", "bbb", "ccc", "ddd"))

        val accDS = words.map(new RichMapFunction[String, String] {

            // 创建累加器
            private val counter = new IntCounter()
            // 成员变量只能在单个slot中累计，如果并行度大于1，那么每个slot都是从0开始计数，slot之间互不影响
            var num = 0

            override def open(parameters: Configuration): Unit = {
                // 注册累加器
                getRuntimeContext.addAccumulator("lines", counter)
            }

            override def map(value: String): String = {
                // 使用累加器
                counter.add(1)
                num += 1
                value
            }

            override def close(): Unit = {
                println(num)
            }
        }).setParallelism(2)

        accDS.writeAsText("/Users/mw/temp/temp", FileSystem.WriteMode.OVERWRITE)

        val execute: JobExecutionResult = env.execute("Acc")

        // 任务执行过程中不可查看累加器，必须等程序执行完成才能查看
        val i = execute.getAccumulatorResult[Int]("lines")
        println(s"acc: ${i}")

    }
}
