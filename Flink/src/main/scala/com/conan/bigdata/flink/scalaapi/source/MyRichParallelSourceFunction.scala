package com.conan.bigdata.flink.scalaapi.source

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._

/**
  * 提供open，close方法，可以打开关闭数据源，比如：打开关闭数据连接
  */
class MyRichParallelSourceFunction extends RichParallelSourceFunction[Long] {

    private var isRunning = true
    private var ele: Long = 0

    override def open(parameters: Configuration): Unit = {
        // 执行一次，可以用来打开数据源
        println("open ...")
    }

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

    override def close(): Unit = {
        // 执行一次，可以关闭数据源
        println("close ...")
    }
}

object MyRichParallelSourceFunction {
    def main(args: Array[String]): Unit = {
        val senv = StreamExecutionEnvironment.getExecutionEnvironment
        val source = senv.addSource(new MyRichParallelSourceFunction).setParallelism(2)  // 并行数据源可以设置并行度
        source.print()

        senv.execute()
    }
}