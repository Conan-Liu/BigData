package com.conan.bigdata.spark.ml.recommendation

import com.conan.bigdata.spark.utils.SparkVariable
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.jblas.FloatMatrix

/**
  * spark ml包实现的协同过滤
  * 删除了[[org.apache.spark.mllib.recommendation.MatrixFactorizationModel]]
  * 引入[[org.apache.spark.ml.recommendation.ALSModel]]
  *
  * 新版的ALS是处理Dataset的，所以不用使用老的Rating，可以自定义case class
  */
object ALSExp extends SparkVariable {

    case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)

    def main(args: Array[String]): Unit = {
        import spark.implicits._
        spark.conf.set("spark.sql.shuffle.partitions", "2")
        val data = spark.read.textFile("E:\\BigData\\Spark\\ml\\ml-latest-small\\ratings.csv").map(x => {
            try {
                val ss = x.split(",")
                Rating(ss(0).toInt, ss(1).toInt, ss(2).toFloat, ss(3).toLong)
            } catch {
                case _: Exception => Rating(0, 0, 0, 0)
            }
        }).filter(_.userId > 0)

        // 划分训练集和测试集
        val Array(trainingData, testData) = data.randomSplit(Array[Double](0.8, 0.2))

        val als = new ALS()
            .setMaxIter(10)
            .setRank(10)
            .setRegParam(0.01)
            .setUserCol("userId")
            .setItemCol("movieId")
            .setRatingCol("rating")
            .setNonnegative(true)

        // fit 底层还是调用train，训练数据得到模型
        val model: ALSModel = als.fit(trainingData)
        // 可以设置冷启动策略
        model.setColdStartStrategy("drop")

        // 用户和物品的特征矩阵
        val userFeatures = model.userFactors
        val itemFeatures = model.itemFactors
        itemFeatures.show(10, false)
        // 根据物品的特征矩阵计算物品的相似度
        // 注意，如果要实现全量的物品相似度，那么需要笛卡儿积
        // 这里自己与自己笛卡儿积，字段名会重复，使用会报错，所以重命名一下
        val itemCross = itemFeatures.crossJoin(itemFeatures).toDF("id1", "feature1", "id2", "feature2")
        val simDF = itemCross.map(x => {
            val id1 = x.getAs[Int]("id1")
            // RDD里面是Array[Float]，DataFrame里面需要使用Seq来接收，再toArray
            val feature1 = x.getAs[Seq[Float]]("feature1").toArray
            val id2 = x.getAs[Int]("id2")
            val feature2 = x.getAs[Seq[Float]]("feature2").toArray
            val id1Vector = new FloatMatrix(feature1)
            val id2Vector = new FloatMatrix(feature2)
            val sim = id1Vector.dot(id2Vector) / (id1Vector.norm2() * id2Vector.norm2())
            (id1, id2, sim)
        }).toDF("id1", "id2", "sim")
        // 直接使用 $"field1" 来表示DataFrame里面的Column
        // 这里类似sql语句的  partition by id1 order by sim desc
        val w = Window.partitionBy($"id1").orderBy($"sim".desc)
        // 计算得到物品两两之间的相似度，最相似的自然是自己和自己，可以考虑过滤掉这部分数据
        // 这里使用 row_number() 函数，需要引入 org.apache.spark.sql.functions._
        val topNSimDF = simDF.withColumn("rn", row_number().over(w)).where($"rn" <= 6)
        topNSimDF.show(20, false)

        // 评估模型
        val predictions = model.transform(testData)
        val evaluator = new RegressionEvaluator()
            .setMetricName("rmse") // 这里只能设置一个
            .setLabelCol("rating")
            .setPredictionCol("prediction")
        val rmse = evaluator.evaluate(predictions)
        println(s"Root Mean Square Error = ${rmse}")

        // 推荐，新版只能给全部用户推荐物品
        model.recommendForAllItems(10)
        model.recommendForAllUsers(10)

        // 保存模型
        // model.save("")
        // 加载模型
        // ALSModel.load("")
    }

}