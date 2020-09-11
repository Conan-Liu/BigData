package com.conan.bigdata.spark.sql.project.processor

import com.conan.bigdata.spark.sql.project.Processor

class Hello extends Processor {

    override def reset(targetDate: String) = {
        println("reset")
    }

    override def execute(targetDate: String): Unit = {
        println(spark.sparkContext.getConf.get("spark.app.name")+"===============")
    }
}
