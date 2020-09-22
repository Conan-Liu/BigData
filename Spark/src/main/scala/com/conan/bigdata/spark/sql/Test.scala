package com.conan.bigdata.spark.sql

import com.conan.bigdata.spark.utils.SparkVariable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.functions._

/**
  */
object Test extends SparkVariable {

    def main(args: Array[String]): Unit = {
        var df: DataFrame = spark.range(10).toDF()
        RuleExecutor.resetTime()
        val start1 = System.currentTimeMillis()
        for (i <- 1 to 200) {
            df = df.withColumn("id_" + i, col("id") + 1)
        }
        val end1 = System.currentTimeMillis()
        println(end1 - start1)
        df.show(false)
        println(RuleExecutor.dumpTimeSpent())

//        RuleExecutor.resetTime()
//        val start2=System.currentTimeMillis()
//                val d2 = df.select((1 to 200).map(i => {
//                    (col("id") + i).as("id_" + i)
//                }): _*)
//        val end2=System.currentTimeMillis()
//        println(end2-start2)
//        d2.show(false)
//        println(RuleExecutor.dumpTimeSpent())

    }
}
