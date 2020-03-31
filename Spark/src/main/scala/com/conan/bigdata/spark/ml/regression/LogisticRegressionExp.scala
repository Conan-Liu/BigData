package com.conan.bigdata.spark.ml.regression

import com.conan.bigdata.spark.utils.SparkVariable
import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.LogisticRegressionDataGenerator

/**
  */
object LogisticRegressionExp extends SparkVariable {

    def main(args: Array[String]): Unit = {

        generateData(sc)

    }

    /**
      * 调用自带工具，生成样本数据
      *
      * @param sc
      */
    def generateData(sc: SparkContext): Unit = {
        val logisticData = LogisticRegressionDataGenerator.generateLogisticRDD(sc, 100, 5, 1.0, 2)
        println(s"总数: ${logisticData.count()}")
        logisticData.take(5).foreach(println)
    }
}