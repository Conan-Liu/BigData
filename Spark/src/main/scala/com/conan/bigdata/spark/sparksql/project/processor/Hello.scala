package com.conan.bigdata.spark.sparksql.project.processor

import com.conan.bigdata.spark.sparksql.project.Processor

class Hello extends Processor {

    override def reset(targetDate: String) = {
        println("reset")
    }

    override def execute(targetDate: String): Unit = {
        println(spark.sparkContext.getConf.get("spark.app.name")+"===============")
    }
}
