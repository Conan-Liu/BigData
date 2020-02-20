package com.conan.bigdata.spark.ml

import com.conan.bigdata.spark.utils.SparkVariable
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

/**
  */
object ALSTest extends SparkVariable {
    val ratingsPath: String = "hdfs://nameservice1/tmp/hive/hive/test/ratings.csv"

    def main(args: Array[String]): Unit = {

        // 等到用户对电影的评分矩阵，生成Rating对象
        val source = sc.textFile(ratingsPath)
        println(s"源数据样例: \n${source.take(5).mkString("\n")}")
        println(s"总记录数: ${source.count()}")

        val ratingsRDD = source.map(x => {
            val Array(userId, movieId, rating, _) = x.split("::")
            try {
                Rating(userId.toInt, movieId.toInt, rating.toDouble)
            } catch {
                case _: Exception => Rating(0, 0, 0)
            }
        }).filter(x => x.user > 0 && x.product > 0)
        println(s"Rating数据样例: ${ratingsRDD.first()}")
        println(s"记录数: ${ratingsRDD.count()}")

        val alsModel: MatrixFactorizationModel = ALS.train(ratingsRDD, 8, 10, 0.01)
        alsModel.save(sc, s"hdfs://nameservice1/tmp/hive/hive/test/model/${System.currentTimeMillis()}")

        // 为所有用户推荐电影，这将耗费大量内存
        println("为所有用户推荐四部电影如下:")
        val rmdMoviesForUsers = alsModel.recommendProductsForUsers(4)
        rmdMoviesForUsers.saveAsTextFile("hdfs://nameservice1/tmp/hive/hive/test/rmd/")

        sc.stop()
    }
}