package com.conan.bigdata.spark.ml

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * MLlib包下面的ALS实现推荐算法
  *
  * http://spark.apache.org/docs/2.3.0/mllib-collaborative-filtering.html
  */
object RecommendationExample {

    private val LATEST = "E:\\BigData\\Spark\\ml\\ml-latest\\ratings.csv"
    private val LATEST_SMALL = "E:\\BigData\\Spark\\ml\\ml-latest-small\\ratings.csv"
    private val MODEL_PATH = "E:\\BigData\\Spark\\ml\\collaborative"

    def intFormat(num: String): Int = {
        try {
            num.toInt
        } catch {
            case _: Exception => 0
        }
    }

    def doubleFormat(num: String): Double = {
        try {
            num.toDouble
        } catch {
            case _: Exception => 0.0
        }
    }

    def main(args: Array[String]): Unit = {
        //设置日志输出级别，省的控制台全是没用的日志
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("java.lang").setLevel(Level.WARN)
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.spark_project.jetty").setLevel(Level.WARN)

        val sparkConf = new SparkConf().setAppName("RecommendationExample").setMaster("local[*]")
        val sc = new SparkContext(sparkConf)

        val data = sc.textFile(LATEST_SMALL)
        data.take(10).foreach(println)
        println("总记录数: " + data.count())
        val ratings = data.map(_.split(',') match {
            case Array(user, product, rating, timestamp) => Rating(intFormat(user), intFormat(product), doubleFormat(rating))
            case _ => Rating(0, 0, 0)
        }).filter(x => if (x.user > 0 && x.product > 0) true else false)
        ratings.cache()
        println("有效记录数: " + ratings.count())
        ratings.take(10).foreach(println)

        // 使用 ALS 构建推荐模型
        val rank = 10
        val numIterations = 10
        val model = ALS.train(ratings, rank, numIterations, 0.01)

        // 评估模型
        val userProducts = ratings.map(x => (x.user, x.product))
        val predictions = model.predict(userProducts).map(x => ((x.user, x.product), x.rating))
        val ratesAndPreds = ratings.map(x => ((x.user, x.product), x.rating)).join(predictions)
        // MSE 评估： 预测分和实际分的差值平方的平均值
        val MSE = ratesAndPreds.map {
            case ((user, product), (r1, r2)) =>
                val err = r1 - r2
                err * err
        }.mean()
        println(s"MSE 分值 : $MSE")

        // 保存和加载模型
        val modelFile = new File(MODEL_PATH)
        modelFile.deleteOnExit()
        model.save(sc, MODEL_PATH)
        val sameModel = MatrixFactorizationModel.load(sc, MODEL_PATH)

        sc.stop()
    }
}