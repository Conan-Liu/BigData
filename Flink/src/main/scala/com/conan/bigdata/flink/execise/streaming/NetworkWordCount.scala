package com.conan.bigdata.flink.execise.streaming

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * WIN 环境
  * nc -lL -p 9999
  */
object NetworkWordCount {

    def main(args: Array[String]): Unit = {
        // Flink自带工具类获取命令行参数
        val tool = ParameterTool.fromArgs(args)
        val host = tool.get("host", "192.168.1.8")
        val port = tool.getInt("port", 9999)
        // 流处理程序，需要创建StreamExecutionEnvironment来处理，相当于Context
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val text = env.socketTextStream(host, port)
        // 这里有个TypeInformation的隐式转换
        // keyBy()可以支持多个字段汇总，也就是说和sql一样groupby多个字段，不需要再像spark样，把所有groupby的字段封装成一个k
        val counts = text.flatMap(_.toLowerCase.split("\\s+"))
            .map((_, 1)).keyBy(0).timeWindow(Time.seconds(1)).sum(1)

        counts.print()
        env.execute(getClass.getName)
    }
}