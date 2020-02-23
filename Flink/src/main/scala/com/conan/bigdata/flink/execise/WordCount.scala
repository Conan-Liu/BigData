package com.conan.bigdata.flink.execise

import org.apache.flink.api.scala._

/**
  */
object WordCount {

    def main(args: Array[String]): Unit = {
        // 批处理程序，需要创建ExecutionEnvironment
        val env = ExecutionEnvironment.getExecutionEnvironment
        // 这里有个隐式转换
        val text = env.fromElements[String]("Who's there?","I think I hear them. Stand, ho! Who's there?","haha")

        val counts=text.flatMap(_.toLowerCase.split("\\W+")).filter(_.nonEmpty)
            .map((_,1)).groupBy(0).sum(1)

        counts.print()
    }
}