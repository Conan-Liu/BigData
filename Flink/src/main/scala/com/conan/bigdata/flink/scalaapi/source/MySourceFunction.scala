package com.conan.bigdata.flink.scalaapi.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._

/**
  * 演示自定义的非并行数据源，实现接口[[SourceFunction]]
  */
class MySourceFunction extends SourceFunction[Long] {

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

object MySourceFunction{
    def main(args: Array[String]): Unit = {
        val senv=StreamExecutionEnvironment.getExecutionEnvironment
        val source = senv.addSource(new MySourceFunction) // .setParallelism(2)  非并行数据源并行度只能为1，否则报错
        source.print()

        senv.execute()
    }
}