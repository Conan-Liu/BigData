package com.conan.bigdata.spark.ml

import com.conan.bigdata.spark.utils.SparkVariable
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

// Vector是scala默认引入的类，spark的Vector重命名下
import org.apache.spark.mllib.linalg.{Vector => SparkVector}
import org.apache.spark.rdd.RDD

/**
  * 机器学习的一些基本的统计信息
  *
  * mllib包下面的内容比ml包的丰富
  */
object BasicStatisticsExp extends SparkVariable {

    def main(args: Array[String]): Unit = {
        val vector = sc.parallelize(Seq[SparkVector](
            Vectors.dense(1, 13, 100),
            Vectors.dense(2, 20, 0),
            Vectors.dense(3, 30, 300)
        ))

        val series1 = sc.parallelize(Seq[Double](1, 2, 3, 3, 5))
        val series2 = sc.parallelize(Seq[Double](11, 22, 33, 33, 555))

        println("*********** Summary statistics **************")
        summaryStatistics(vector)

        println("*********** Correlations **************")
        correlations(series1, series2, vector)

        println("*********** Stratified sampling **************")
        stratifiedSampling(sc)

        println("*********** Random data generation **************")
        randomDataGeneration(sc)
    }

    /**
      * 针对向量列的摘要统计信息
      */
    def summaryStatistics(vector: RDD[SparkVector]): Unit = {
        val summary: MultivariateStatisticalSummary = Statistics.colStats(vector)
        println(s"列平均值 = ${summary.mean}")
        println(s"列方差 = ${summary.variance}")
        println(s"列非0个数 = ${summary.numNonzeros}")
        println(s"列L2标准 = ${summary.normL2}")
    }

    /**
      * 向量或两个序列，计算向量之间，序列之间的相关系数
      * 向量之间的相关系数构成的是相关矩阵
      * 相关矩阵也叫相关系数矩阵，是由矩阵各列间的相关系数构成的。也就是说，相关矩阵第i行第j列的元素是原矩阵第i列和第j列的相关系数
      */
    def correlations(series1: RDD[Double], series2: RDD[Double], vector: RDD[SparkVector]): Unit = {
        // 两种计算相关系数的算法，皮尔逊系数(Pearson)和斯皮尔曼(Spearman)，默认Pearson
        val seriesCorrelation = Statistics.corr(series1, series2, "pearson")
        println(s"seriesCorrelation = ${seriesCorrelation}")

        val matrixCorrelation = Statistics.corr(vector, "pearson")
        println(s"matrixCorrelation =\n${matrixCorrelation}")
    }

    /**
      * 分层抽样
      * 分层取样（Stratified sampling）顾名思义，就是将数据根据不同的特征分成不同的组，然后按特定条件从不同的组中获取样本，并重新组成新的数组
      * 抽样算子sample sampleByKey
      * withReplacement：表示抽出样本后是否在放回去，true表示会放回去，这也就意味着抽出的样本可能有重复
      * fraction ：参数来定义分类条件和采样几率。fractions参数被定义成一个Map[K, Double]类型，K是键值的分层条件，Double是该满足条件的K条件的采样比例，1.0代表 100%
      *
      * sampleByKey 方法需要作用于一个键值对数组，其中 key 用于分类
      * sampleByKeyExtra 采样的结果会更准确，有99.99%的置信度，但耗费的计算资源也更多
      */
    def stratifiedSampling(sc: SparkContext): Unit = {
        val data = sc.parallelize(Seq[(Int, String)]((1, "a"), (1, "b"), (2, "c"), (2, "d"), (2, "e"), (3, "f")))
        val fractions = Map(1 -> 0.1, 2 -> 0.6, 3 -> 0.3)

        val approxSample = data.sampleByKey(withReplacement = false, fractions = fractions)
        approxSample.take(10).foreach(println)
    }


    /**
      * 生成随机数
      * 包括随机序列和随机向量
      */
    def randomDataGeneration(sc: SparkContext): Unit = {
        // 随机序列
        val seriesRandom = RandomRDDs.normalRDD(sc, 100000L, 2)
        println(s"随机序列\n ${seriesRandom.take(10).mkString(",")}")

        val vectorRandom = RandomRDDs.normalVectorRDD(sc, 4, 5, 2)
        println("随机向量")
        vectorRandom.take(5).foreach(println)
    }
}