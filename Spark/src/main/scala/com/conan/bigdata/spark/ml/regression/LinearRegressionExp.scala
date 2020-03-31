package com.conan.bigdata.spark.ml.regression

import com.conan.bigdata.spark.utils.SparkVariable
import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.LinearDataGenerator

/**
  */
object LinearRegressionExp extends SparkVariable{

    def main(args: Array[String]): Unit = {

        generateData(sc)
    }

    /**
      * 调用自带工具，生成样本数据
      * @param sc
      */
    def generateData(sc:SparkContext): Unit ={
        val linearData=LinearDataGenerator.generateLinearRDD(sc,100,5,1.0,2)
        println(s"总数: ${linearData.count()}")
        linearData.take(5).foreach(println)
    }
}