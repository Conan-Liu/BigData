package com.conan.bigdata.flink.scalaapi.source

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._

/**
  * 自定义的并行数据源，实现[[ParallelSourceFunction]]
  * 并行和非并行数据源代码一致，区别就是实现不同的接口
  * 并行发送数据是重复的，重复度就是对应的并行度，举个例子，并行度为8（默认core数），数据就会重复8次
  */
class MyParallelSourceFunction extends ParallelSourceFunction[Long] {

    private var isRunning = true
    private var ele: Long = 0

    override def run(ctx: SourceContext[Long]): Unit = {
        while (isRunning) {
            ele += 1
            ctx.collect(ele)
            // 发送一条睡眠1秒
            Thread.sleep(1000)
        }
    }

    override def cancel(): Unit = {
        // 取消发送数据
        isRunning = false
    }
}

object MyParallelSourceFunction {
    def main(args: Array[String]): Unit = {
        val senv = StreamExecutionEnvironment.getExecutionEnvironment
        val source = senv.addSource(new MyParallelSourceFunction).setParallelism(2)  //并行数据源可以设置并行度
        source.print()

        senv.execute()
    }
}