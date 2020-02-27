package com.conan.bigdata.flink.execise.streaming

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * WIN 环境
  * nc -lL -p 9999
  */
object NetworkWordCount {

    def main(args: Array[String]): Unit = {
        // 流处理程序，需要创建StreamExecutionEnvironment来处理，相当于Context
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val text = env.socketTextStream("localhost", 9999)
        // 这里有个TypeInformation的隐式转换
        val counts=text.flatMap(_.toLowerCase.split("\\s+"))
            .map((_,1)).keyBy(0).timeWindow(Time.seconds(1)).sum(1)


        counts.print()
        env.execute(getClass.getName)
    }
}