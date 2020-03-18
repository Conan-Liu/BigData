package com.conan.bigdata.spark.ml.recommendation

import java.io.File

import com.conan.bigdata.spark.utils.SparkVariable
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

/**
  * MLlib包下面的ALS实现推荐算法
  *
  * http://spark.apache.org/docs/2.3.0/mllib-collaborative-filtering.html
  */
object RecommendationExample extends SparkVariable{

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
        val data = sc.textFile(LATEST_SMALL)
        data.take(10).foreach(println)
        println("总记录数: " + data.count())
        val ratings = data.map(_.split(',') match {
            case Array(user, product, rating, timestamp) => Rating(intFormat(user), intFormat(product), doubleFormat(rating))
            case _ => Rating(0, 0, 0)
        }).filter(x => if (x.user > 0 && x.product > 0) true else false)
        val userProducts = ratings.map(x => (x.user, x.product))
        ratings.cache()
        userProducts.cache()
        println("有效记录数: " + ratings.count())
        ratings.take(10).foreach(println)

        // 使用 ALS 构建推荐模型
        val rank = 10
        val iteration = Seq(10, 15, 20)
        val lambda = Seq(0.001, 0.005, 0.01, 0.05, 0.1)
        // 这个评分用来记录比较好的模型评分
        var bestRMSE = Double.MaxValue
        // 多次循环计算模型，得到一个MSE评分最佳的模型
        // 有时并不能得到最佳模型， 只需要满足收敛条件即可
        for (i <- iteration; l <- lambda) {
            // ratings 这个RDD最好持久化起来，打断依赖，加快执行速度
            val model = ALS.train(ratings, rank, i, l)
            // 评估模型
            val predictions = model.predict(userProducts).map(x => ((x.user, x.product), x.rating))
            val ratesAndPreds = ratings.map(x => ((x.user, x.product), x.rating)).join(predictions)
            // MSE 评估： 预测分和实际分的差值平方的平均值
            val MSE = ratesAndPreds.map {
                case ((user, product), (r1, r2)) =>
                    val err = r1 - r2
                    err * err
            }.mean()
            println(s"MSE 分值 : $MSE")
            // RMSE 是MSE的开方
            val RMSE = math.sqrt(MSE)
            println(s"RMSE 分值 : $RMSE")

            // 这个评分越小, 代表模型越精确, 满足条件则把模型保存下来
            // 这里面越精确， 可能会存在 过拟合 问题
            // 假设我今天买了一台电脑， 那么另外一台一模一样的电脑
            // 评分最接近， 如果继续把这台电脑推荐给我，那么这种在实际情况下是不合适的
            if (RMSE < bestRMSE) {
                // 反复循环计算模型,得到越来越小的模型RMSE, 可以把iteration次数和lambda记录下来
                bestRMSE = RMSE
                println(s"越来越小的RMSE: $bestRMSE")
                // 保存和加载模型， 保存再hdfs上
                val modelFile = new File(MODEL_PATH)
                modelFile.deleteOnExit()
                                model.save(sc, s"$MODEL_PATH/$RMSE")
                //                val sameModel = MatrixFactorizationModel.load(sc, MODEL_PATH)
            }
        }



        sc.stop()
    }

    // 使用训练的模型为一个用户推荐5部电影
    def recommendUserProduct(sc:SparkContext,movies:RDD[(Int,String)]):Unit={
        // 加载模型
        val model=MatrixFactorizationModel.load(sc,MODEL_PATH)
        // 随机从训练集里面找一个用户id来推荐 如 123, 推荐 5部电影, 返回一个Rating 数组
        // 调用sc.parallelize(Seq)将数组转为RDD, 当然也可以直接foreach遍历这个数组
        val userProduct= sc.parallelize(model.recommendProducts(123,5).map(x=>(x.product,x.user)))
        // 上面计算得到针对用户123推荐的5部电影id， 可以直接关联电影表找到电影名称， 也可以直接保存电影id
        val userProductWithName=userProduct.join(movies)
        println("为用户 123 推荐了五部电影， 如下")
        userProductWithName.foreach(x=>{
            println(s"${x._1} : ${x._2._2}")
        })

        // 表示为模型中所有用户推荐 5部电影
        // 注意： 这个方法需要先得到用户对电影的喜好程度矩阵，然后针对每个用户，也就是每行排序取前5条
        val allUserProducts=model.recommendProductsForUsers(5)
    }
}