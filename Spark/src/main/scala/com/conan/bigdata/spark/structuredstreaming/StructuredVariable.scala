package com.conan.bigdata.spark.structuredstreaming

import org.apache.spark.sql.SparkSession

trait StructuredVariable {

    val spark: SparkSession = SparkSession.builder().appName(getClass.getSimpleName).master("local[4]").config("word.config", "222").getOrCreate()

}
