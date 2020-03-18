package com.conan.bigdata.spark.ml.recommendation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * movies.dat 样例
  * 电影id   电影标题   电影类别
  * 1::Toy Story (1995)::Adventure|Animation|Children|Comedy|Fantasy
  * 2::Jumanji (1995)::Adventure|Children|Fantasy
  *
  * ratings.dat 样例
  * 用户id  电影id  评分  提交时间
  * 1::122::5::838985046
  * 1::185::5::838983525
  */

object MovieRecommendation {

    case class Ratings(userId: Int, movieId: Int, score: Double)

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("").setMaster("local[4]")
        val sc = new SparkContext(sparkConf)

        // 电影数据
        val movie = sc.textFile("D:\\Spark\\ml\\ml-10M100K\\movies.dat").map(line => {
            val str = line.split("::")
            (str(0).toInt, str(1))
        })
        movie.take(10).foreach(println)

        // 评分数据
        val rate = sc.textFile("D:\\Spark\\ml\\ml-10M100K\\ratings.dat", 10).map(line => {
            val str = line.split("::")
            val rating = Ratings(str(0).toInt, str(1).toInt, str(2).toDouble)
            val times = str(3).toLong % 10
            (times, rating)
        })

        val movieCnt = rate.map(_._2.movieId).distinct().count()
        val userCnt = rate.map(_._2.userId).distinct().count()
        val totalCnt = rate.count()

        println(s"一共有${totalCnt}条数据，其中电影条数: $movieCnt 用户数: $userCnt")

        // 划分训练集和测试集
        val rateSplits = rate.randomSplit(Array(0.2, 0.8), 2)

    }

}