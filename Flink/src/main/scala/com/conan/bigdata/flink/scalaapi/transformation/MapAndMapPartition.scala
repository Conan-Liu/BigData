package com.conan.bigdata.flink.scalaapi.transformation

import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala._

/**
  *
  */
object MapAndMapPartition {

    case class Student(id: Int, name: String)

    def main(args: Array[String]): Unit = {
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val sourceDS: DataSet[String] = env.fromCollection[String](List("1,张三", "2,李四", "3,王二", "4,麻子"))

        val stuDS: DataSet[Student] = sourceDS.map(x => {
            val split: Array[String] = x.split(",")
            Student(split(0).toInt, split(1))
        })

        val stuDS1 = sourceDS.mapPartition(iter => {
            // mapPartition传入一个Iterator，一个分区执行一次，可以做一些比较昂贵的操作，如数据库链接
            iter.map(x => {
                val split: Array[String] = x.split(",")
                Student(split(0).toInt, split(1))
            })
            // 关闭链接
        })

        // flatMap返回类型，一定要是可迭代的数据类型，如Array，Set，List
        val flatMapDS=sourceDS.flatMap(x=>{
            x.split(",")
        })

        // 这里方法和sum(1)相同，该方法里目前只支持sum max min
        sourceDS.aggregate(Aggregations.SUM,1)

        stuDS.print()
        stuDS1.print()
    }
}
