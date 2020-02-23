package com.conan.bigdata.spark.ml

import com.conan.bigdata.spark.utils.SparkVariable
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

/**
  */
object ALSCF extends SparkVariable {

    val ratingsPath: String = "E:\\Apache\\Spark\\ml\\ml-1m\\ratings.dat"

    def main(args: Array[String]): Unit = {

        // 最好使用Kryo序列化，注册下序列化的类
        sc.getConf.registerKryoClasses(Array(classOf[Rating]))

        // 等到用户对电影的评分矩阵，生成Rating对象
        val source = sc.textFile(ratingsPath)
        println(s"源数据样例: \n${source.take(5).mkString("\n")}")
        println(s"总记录数: ${source.count()}")

        val ratingsRDD = source.map(x => {
            val Array(userId, movieId, rating, _) = x.split("::")
            Rating(userId.toInt, movieId.toInt, rating.toDouble)
        }).filter(x => !(Option(x.user).isEmpty || Option(x.product).isEmpty))
        println(s"Rating数据样例: ${ratingsRDD.first()}")
        println(s"记录数: ${ratingsRDD.count()}")
        val distinctUsers = ratingsRDD.map(_.user).distinct().count()
        val distinctMovies = ratingsRDD.map(_.product).distinct().count()
        val distinctUsersMovies = ratingsRDD.map(x => (x.user, x.product)).distinct().count()
        println(s"不重复用户数:${distinctUsers}\n不重复电影数:${distinctMovies}\n不重复用户电影数:${distinctUsersMovies}")


        // 可以分割成训练集和测试集，训练集用于训练模型，测试集用于验证推荐结果准确性
        // 避免过拟合，验证模型泛化能力
        println("***********划分训练集和测试集************")
        val Array(trainRDD, testRDD) = ratingsRDD.randomSplit(Array(0.8, 0.2))
        println(s"训练集总数: ${trainRDD.count()}， 测试集总数: ${testRDD.count()}")
        println(s"****${testRDD.map(x => (x.user, x.product)).distinct().count()}")


        // 调用ALS算法训练数据，属于显示评分训练
        // rank: 这里rank特征值不好理解，对应到数据上来看，就是大矩阵拆分成小矩阵R(m*n) = X(m*k) * Y(k*n)的那个中间纬度k
        //     对应ALS模型中的因子个数，也就是在低阶近似矩阵中的隐含特征个数。因子个
        //     数一般越多越好。但它也会直接影响模型训练和保存时所需的内存开销，尤其是在用户
        //     和物品很多的时候。因此实践中该参数常作为训练效果与系统开销之间的调节参数。通常其合理取值为10到200
        // iterations: 对应运行时的迭代次数。ALS能确保每次迭代都能降低评级矩阵的重建误
        //     差，但一般经少数次迭代后ALS模型便已能收敛为一个比较合理的好模型。这样大部分情况下都没必要迭代太多次（10次左右一般就挺好）
        // lambda: 该参数控制模型的正则化过程，从而控制模型的过拟合情况。其值越高，正则化越严厉，默认0.01
        // seed: 用来控制ALS随即初始化矩阵的值，默认System.nanoTime()
        // 可以用多级循环来不停的计算参数，比较MSE和RMSE的大小，得到一个比较好的模型
        val alsModel: MatrixFactorizationModel = ALS.train(trainRDD, 8, 10, 0.01)


        // 模型评估 MSE RMSE 评估
        // 根据测试集数据得到的 真实评分((user, product), rating))
        // 根据模型计算测试集的 预测评分((user, product), rating))
        // 两个关联得到一个数据集，记录了用户产品对的实际评分和预测评分
        // 根据这个数据集的两个评分计算 MSE和 RMSE，用来评估该模型的好坏，越接近0越佳
        println("**************模型评估************************")
        val actualRatings = testRDD.map(x => ((x.user, x.product), x.rating))
        val predictRatings = alsModel.predict(testRDD.map(x => (x.user, x.product))).map(x => ((x.user, x.product), x.rating))
        println(s"真实用户物品评分总数: ${actualRatings.count()}\n预测用户物品评分总数: ${predictRatings.count()}")
        val predictAndActual: RDD[((Int, Int), (Double, Double))] = actualRatings.join(predictRatings)
        println(s"样例数据: \n${predictAndActual.take(5).mkString(",")}\n")
        val evalModel = new RegressionMetrics(predictAndActual.map(_._2))
        println(s"MSE = ${evalModel.meanSquaredError}")
        println(s"RMSE = ${evalModel.rootMeanSquaredError}")


        // ALS 计算得到两个小矩阵，来近似代表原来的评分矩阵ratingsRDD X * Y = R
        // 获取ALS模型包含的两个小矩阵:
        // 1. 用户因子矩阵userFeatures
        // 2. 产品因子矩阵productFeatures
        println("**************ALS拆分的两个因子矩阵***********************")
        val userFeatures: RDD[(Int, Array[Double])] = alsModel.userFeatures
        val productFeatures: RDD[(Int, Array[Double])] = alsModel.productFeatures
        println(s"用户因子矩阵总数: ${userFeatures.count()}")
        println(s"产品因子矩阵总数: ${productFeatures.count()}") // 这里是产品因子矩阵的转置
        println(s"用户因子矩阵: \n${userFeatures.mapValues(_.mkString("[", ",", "]")).take(5).mkString("\n")}")
        println(s"产品因子矩阵: \n${productFeatures.mapValues(_.mkString("[", ",", "]")).take(5).mkString("\n")}")


        // 预测评分，预测某一个用户对某个电影的评分
        val user1 = 123
        val movie1 = 456
        val predictRating: Double = alsModel.predict(user1, movie1)
        println(s"预测用户${user1}对电影${movie1}的评分: ${predictRating}")


        // 为某一个用户推荐五部电影
        val rmdMovies = alsModel.recommendProducts(user1, 5)
        println(s"为用户${user1}推荐五部电影如下:")
        rmdMovies.foreach(println)


        // 为所有用户推荐电影，经运行，并不会消耗多长时间和内存
        // 推荐结果可以保存为文件供Hive查询，也可以写入RDBMS
        println("为所有用户推荐四部电影如下:")
        val rmdMoviesForUsers = alsModel.recommendProductsForUsers(4)
        println(rmdMoviesForUsers.mapValues(_.mkString("[", ",", "]")).take(5).mkString(","))
        // rmdMoviesForUsers.mapValues(_.mkString("[", ",", "]")).saveAsTextFile("hdfs路径")

        // 保存模型，以便后期加载使用进行推荐，必须是不存在的目录
        // alsModel.save(sc, "hdfs路径")

        // 重新加载之前保存的模型，用于推荐
        // val loadAlsModel = MatrixFactorizationModel.load(sc, "")
        // val loaPredictRating: Double = loadAlsModel.predict(user1, movie1)

        sc.stop()
    }
}