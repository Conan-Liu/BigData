package com.conan.bigdata.spark.ml

import com.conan.bigdata.spark.ml.feature.Word2VecExp._
import com.conan.bigdata.spark.utils.SparkVariable
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.Row

/**
  * 基于 spark ml 包下面的Pipeline类来构建机器学习的工作流
  */
object PipelineWorkflow extends SparkVariable {

    def main(args: Array[String]): Unit = {
        // 训练集
        val trainingData = spark.createDataFrame(Seq[(Int, String, Double)](
            (0, "a b c d e spark", 1.0),
            (1, "b d", 0.0),
            (2, "spark f g h", 1.0),
            (3, "hadoop mapreduce", 0.0)
        )).toDF("id", "text", "label")

        // 定义Pipeline工作流的各个阶段
        val tokenizer = new Tokenizer()
            .setInputCol("text")
            .setOutputCol("words")

        val hashingTF = new HashingTF()
            .setNumFeatures(1000)
            .setInputCol(tokenizer.getOutputCol)
            .setOutputCol("features")

        val lr = new LogisticRegression()
            .setMaxIter(10)
            .setRegParam(0.01)

        // 这些特定问题的转换器和评估器，就可以按照具体的处理逻辑有序的组织生成Pipeline
        // 现在构建的Pipeline本质上是一个Estimator
        val pipeline = new Pipeline()
            .setStages(Array(tokenizer, hashingTF, lr))

        // 在Pipeline调用fit（）方法运行之后，它将产生一个PipelineModel，它是一个Transformer
        val model = pipeline.fit(trainingData)


        // 测试集
        val testData = spark.createDataFrame(Seq[(Int, String)](
            (4, "spark i j k"),
            (5, "l m n"),
            (6, "spark a"),
            (7, "apache hadoop")
        )).toDF("id", "text")

        //调用我们训练好的PipelineModel的transform()方法，让测试数据按顺序通过拟合的工作流，生成我们所需要的预测结果
        val result = model.transform(testData).select("id", "text", "probability", "prediction")
        result.show(100,false)

        spark.stop()
    }

}