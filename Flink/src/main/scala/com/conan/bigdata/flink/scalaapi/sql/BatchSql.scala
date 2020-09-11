package com.conan.bigdata.flink.scalaapi.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
  * 演示Flink SQL批处理
  */
object BatchSql {

    def main(args: Array[String]): Unit = {

        val env = ExecutionEnvironment.getExecutionEnvironment
        val tenv = BatchTableEnvironment.create(env)

        val ds = env.fromCollection(List[(String, Double)](("hadoop", 10.3), ("spark", 9.4), ("hadoop", 11.4), ("spark", 2.7), ("flink", 9)))

        // 注册为数据表
        tenv.registerDataSet[(String, Double)]("data", ds, 'word, 'weight)

        val set: Table = tenv.fromDataSet[(String, Double)](ds, 'word, 'weight)

        val t1: Table = tenv.sqlQuery(
            """
              |select
              |word,
              |weight
              |from data
            """.stripMargin)

        val t1DS = t1.toDataSet[(String,Double)]
        t1DS.print()

        val select: Table = set.groupBy('word).select('word,'weight.sum,'weight.sum0,'weight.count)
        val set1: DataSet[Row] = select.toDataSet[Row]
        set1.print()
    }
}
