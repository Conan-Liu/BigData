package com.conan.bigdata.spark.ml

import com.conan.bigdata.spark.utils.SparkVariable
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

/**
  */
object ALSCF extends SparkVariable {

    def main(args: Array[String]): Unit = {

        // 等到用户对电影的评分矩阵，生成Rating对象
        val ratingsRDD = sc.textFile("").map(x => {
            val Array(userId, movieId, rating, _) = x.split("\\s+")
            Rating(userId.toInt, movieId.toInt, rating.toDouble)
        })
        println(s"数据样例: ${ratingsRDD.first()}")
        println(s"记录数: ${ratingsRDD.count()}")

        // 调用ALS算法训练数据
        val alsModel: MatrixFactorizationModel = ALS.train(ratingsRDD, 10, 10, 1)


        // 模型评估 MSE RMSE 评估
        val evalModel = null


        // ALS 计算得到两个小矩阵，来近似代表原来的评分矩阵ratingsRDD X * Y = R
        // 获取ALS模型宝行的两个小矩阵:
        // 1. 用户因子矩阵userFeatures
        // 2. 产品因子矩阵productFeatures
        val userFeatures: RDD[(Int, Array[Double])] = alsModel.userFeatures
        val productFeatures: RDD[(Int, Array[Double])] = alsModel.productFeatures


        // 预测评分，预测某一个用户对某个电影的评分
        val user1 = 123
        val movie1 = 456
        val predictRating: Double = alsModel.predict(user1, movie1)
        println(s"预测用户${user1}对电影${movie1}的评分: ${predictRating}")


        // 为某一个用户推荐十部电影
        val rmdMovies = alsModel.recommendProducts(user1, 10)
        println(s"为用户${user1}推荐十部电影如下:")
        rmdMovies.foreach(println)


        // 为一批用户推荐电影
        // TODO...


        // 保存模型，以便后期加载使用进行推荐
        // alsModel.save(sc, "")

        // 重新加载之前保存的模型
        // val loadAlsModel = MatrixFactorizationModel.load(sc, "")
        // val loaPredictRating: Double = loadAlsModel.predict(user1, movie1)


        sc.stop()
    }
}