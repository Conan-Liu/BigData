package com.conan.bigdata.flink.scalaapi.transformation

import java.io.File

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

import scala.io.Source

/**
  * 分布式缓存，适用于文件
  * 将小文件缓存到TaskManager上，TaskManager上直接获取分布式缓存，slot共享缓存文件数据
  */
object DistributedCache {

    def main(args: Array[String]): Unit = {
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        // 学生数据集，广播
        val scoreDS = env.fromCollection(List[(Int, String, Int)]((1, "语文", 50), (2, "数学", 70), (3, "英语", 86), (4, "计算机", 96)))

        // 注册学生数据缓存
        env.registerCachedFile("", "student_cache")

        val resDS = scoreDS.map(new RichMapFunction[(Int, String, Int), (String, String, Int)] {
            // 定义成员变量，可以被map方法使用
            var toMap: Map[Int, String] = _

            // 获取缓存文件，只执行一次，并读取文件
            override def open(parameters: Configuration): Unit = {
                val file: File = getRuntimeContext.getDistributedCache.getFile("student_cache")
                toMap = Source.fromFile(file).getLines().map(line => {
                    val arr = line.split(",")
                    (arr(0).toInt, arr(1))
                }).toMap
            }

            override def map(value: (Int, String, Int)): (String, String, Int) = {
                val stuId = value._1
                val stuName = toMap.getOrElse(stuId, "null")
                (stuName, value._2, value._3)
            }
        }).setParallelism(2)

        resDS.print()
    }
}
