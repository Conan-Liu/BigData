package com.conan.bigdata.flink.scalaapi.transformation

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

import scala.collection.JavaConverters._
import scala.collection.immutable

/**
  * Flink的广播变量
  */
object Broadcast {

    def main(args: Array[String]): Unit = {
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        // 学生数据集，广播
        val studentDS = env.fromCollection(List[(Int, String)]((1, "张三"), (2, "李四"), (3, "王二")))
        val scoreDS = env.fromCollection(List[(Int, String, Int)]((1, "语文", 50), (2, "数学", 70), (3, "英语", 86), (4, "计算机", 96)))

        val resDS = scoreDS.map(new RichMapFunction[(Int, String, Int), (String, String, Int)] {
            // 定义成员变量获取广播数据
            var stuBC: immutable.Map[Int, String] = _

            // 获取广播变量，该方法只执行一次
            override def open(parameters: Configuration): Unit = {
                stuBC = getRuntimeContext.getBroadcastVariable[(Int, String)]("student").asScala.toMap

            }

            override def map(value: (Int, String, Int)): (String, String, Int) = {
                val stuId = value._1
                val stuName = stuBC.getOrElse(stuId, "null")
                (stuName, value._2, value._3)
            }

        }).withBroadcastSet(studentDS, "student") // 定义广播数据


        resDS.print()
    }
}
