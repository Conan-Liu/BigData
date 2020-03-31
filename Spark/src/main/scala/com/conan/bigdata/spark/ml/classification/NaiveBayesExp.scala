package com.conan.bigdata.spark.ml.classification

import com.conan.bigdata.spark.utils.SparkVariable
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

/**
  * 朴素贝叶斯概念: 对于给出的待分类项x，求解在此项出现的条件下各个类别出现的概率，哪个最大，就认为x属于哪个类别。
  * 主要有三种模型
  * 多项式模型
  * 在多项式朴素贝叶斯模型中，特征向量的特征通常为离散型变量，并且假定所有特征的取值是符合多项分布的。如“年龄”包括：青年、中年、老年。
  * 伯努利模型
  * 在伯努利朴素贝叶斯模型中，每个特征的取值是布尔型，或以0和1表示，所以伯努利模型中，每个特征值为0或者1。 当数据均为0和1组成，则可使用该模型。
  * 高斯模型
  * 在高斯朴素贝叶斯模型中，特征向量的特征为连续型变量，并且假定所有特征的取值是符合高斯分布的
  *
  * Spark MLlib目前只支持多项模型和伯努利模型，没有支持高斯模型
  */
object NaiveBayesExp extends SparkVariable {

    def main(args: Array[String]): Unit = {

        val data = spark.read.format("libsvm").load("E:\\BigData\\Spark\\spark-2.2.3-src\\data\\mllib\\sample_libsvm_data.txt")
        data.printSchema()
        val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2), seed = 1234L)

        // 训练模型
        val model = new NaiveBayes().fit(trainingData)

        // 预测
        val predictions = model.transform(testData)
        predictions.show(10, false)

        // 评估模型
        val evaluator = new MulticlassClassificationEvaluator()
            .setLabelCol("label")
            .setPredictionCol("prediction")
            .setMetricName("accuracy")
        val accuracy = evaluator.evaluate(predictions)
        println("Test set accuracy = " + accuracy)

    }
}