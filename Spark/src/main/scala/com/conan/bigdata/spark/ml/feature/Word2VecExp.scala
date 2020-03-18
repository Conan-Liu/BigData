package com.conan.bigdata.spark.ml.feature

import com.conan.bigdata.spark.utils.SparkVariable
import org.apache.spark.ml.feature.Word2Vec

/**
  * Word2Vec具有两种模型，其一是 CBOW ，其思想是通过每个词的上下文窗口词词向量来预测中心词的词向量。其二是 Skip-gram，其思想是通过每个中心词来预测其上下文窗口词，并根据预测结果来修正中心词的词向量
  */
object Word2VecExp extends SparkVariable {

    def main(args: Array[String]): Unit = {

        // TODO...  搞不懂这什么意思
        val documentDF = spark.createDataFrame(Seq(
            "Hi I heard about Spark".split(" "),
            "I wish Java could use case classes".split(" "),
            "Logistic regression models are neat".split(" ")
        ).map(Tuple1.apply)).toDF("text")
        documentDF.show(100,false)

        val word2Vec=new Word2Vec()
            .setInputCol("text")
            .setOutputCol("result")
            .setVectorSize(3)
            .setMinCount(0)

        // 读入训练数据，用fit()方法生成一个Word2VecModel
        val model=word2Vec.fit(documentDF)

        // 利用Word2VecModel把文档转变成特征向量
        // 应该使用测试集测试模型，就不多赘述了
        val result=model.transform(documentDF)
        result.show(100,false)

        spark.stop()
    }
}