package com.conan.bigdata.spark.ml.cluster

import com.conan.bigdata.spark.utils.SparkVariable
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

/**
  * 1、首先确定一个k值，即我们希望将数据集经过聚类得到k个集合
  * 2、从数据集中随机选择k个数据点作为质心
  * 3、对数据集中每一个点，计算其与每一个质心的距离（如欧式距离），离哪个质心近，就划分到那个质心所属的集合
  * 4、把所有数据归好集合后，一共有k个集合。然后重新计算每个集合的质心
  * 5、如果新计算出来的质心和原来的质心之间的距离小于某一个设置的阈值（表示重新计算的质心的位置变化不大，趋于稳定，或者说收敛），我们可以认为聚类已经达到期望的结果，算法终止
  * 6、如果新质心和原质心距离变化很大，需要迭代3~5步骤
  *
  * 所以综上所述：该算法的难度在于如何将要聚类的对象进行向量化，其它的都是套路编程
  */
object KMeansExp extends SparkVariable {

    val PATH = "E:\\BigData\\Spark\\ml\\customers_data.csv"

    def main(args: Array[String]): Unit = {

        val sourceRDD = sc.textFile(PATH)
        val dataRDD = sourceRDD.map(_.split(",")).filter(_.length == 8).map(x => Vectors.dense(x.map(_.toDouble)))
        val Array(trainingRDD, testRDD) = dataRDD.randomSplit(Array(0.8, 0.2))
        // 迭代算法，可以考虑把数据cache起来，提升性能
        trainingRDD.cache()
        println(s"训练集总数: ${trainingRDD.count()}, 测试集总数: ${testRDD.count()}")

        val iterations = 10
        // 利用肘部法确定K值，把(K, WSSSE)在excel中画图，得到K=5比较合理
        for (i <- 2 to 10) {
            // 如果traininggRDD没有数据，会报错
            val modelTest = KMeans.train(trainingRDD, i, iterations)
            /**
              * 畸变程度：每个类别距离其该类中心点的距离称为畸变程度
              * 肘部法确定K值：通过慢慢增大K，来计算WSSSE，把(K，WSSSE)在平面直角坐标系下标识出来，连成一条线，当变化幅度不大时，则该K为最优值
              * 这个是为了得到K值，可以另外计算好了直接代入train方法
              */
            val WSSSE = modelTest.computeCost(trainingRDD)
            println(s"K=${i} 的WSSSE值是: ${WSSSE}")
        }

        val K = 5

        val model1 = new KMeans()
            .setK(K)
            .setMaxIterations(iterations)
            .setEpsilon(0.5) // 迭代过程中，质心不停的变化，当变化幅度小于给定值时，表示已经收敛，迭代结束
            .setInitializationMode("k-means||") // 默认是K-means||， 这是优化的k-means，能防止选择的质心集中在一起，影响最终聚类的质量，其思想是令初始聚类中心尽可能的互相远离
            .run(dataRDD)
        // 直接调用伴生对象的方法来创建模型，底层还是调用的new KMeans方法，和上面效果相同
        // 新版的KMeans已经不再使用train方法， 直接new
        val model = KMeans.train(trainingRDD, K, iterations)
        val centers = model.clusterCenters
        // 注意 质心 可以是虚拟点，只要能把点分成几类即可，该点可以不存在
        // 获取中心点的坐标
        println("质心如下:")
        centers.zipWithIndex.foreach(println)

        /**
          * 测试集验证，预测一个点和一批数据
          * Vectors.dense()方法可以使用一系列数字来生成一个向量用于框架计算，多少个数字就是多少维，如下是一个8维向量
          * 这里使用Vectors，而不是使用Vector，因为Vector是scala默认引入的类，参考 [[scala.collection.immutable.Vector]]
          */
        val testPointPredict = model.predict(Vectors.dense(2, 1, 10000, 5000, 4000, 1000, 2000, 3000))
        println(s"单个点所在聚类: ${testPointPredict}")
        val testPredict = model.predict(testRDD)
        println(s"前五个点所在聚类: ${testPredict.take(5).mkString(",")}")

        // 保存模型
        // model.save(sc, "")
        // 加载模型
        // val kMeansModel = KMeansModel.load(sc, "")
        // 只是为了演示，这是一个3维向量
        // println(kMeansModel.predict(Vectors.dense(1, 1, 1)))
    }

}