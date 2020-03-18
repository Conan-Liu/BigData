package com.conan.bigdata.spark.ml.feature

import com.conan.bigdata.spark.utils.SparkVariable
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}

/**
  * 特征提取
  * 词频－逆向文件频率（TF-IDF）是一种在文本挖掘中广泛使用的特征向量化方法，它可以体现一个文档中词语在语料库中的重要程度
  */
object TFIDF extends SparkVariable {

    def main(args: Array[String]): Unit = {

        val sentenceData = spark.createDataFrame(Seq[(Int,String)](
            (0, "I heard about Spark and I love Spark"),
            (0, "I wish Java could use case classes"),
            (1, "Logistic regression models are neat")
        )).toDF("label", "sentence")

        // 得到文档集合后，即可用tokenizer对句子进行分词
        val tokenizer=new Tokenizer()
            .setInputCol("sentence")
            .setOutputCol("words")
        val wordsData=tokenizer.transform(sentenceData)
        wordsData.show(100,false)

        // 得到分词后的文档序列后，即可使用HashingTF的transform()方法把句子哈希成特征向量，这里设置哈希表的桶数为2000
        val hashingTF=new HashingTF()
            .setInputCol("words")
            .setOutputCol("rawFeatures")
            .setNumFeatures(2000)
        val featurizedData=hashingTF.transform(wordsData)
        featurizedData.show(100,false)

        // 使用IDF来对单纯的词频特征向量进行修正，使其更能体现不同词汇对文本的区别能力
        val idf=new IDF()
            .setInputCol("rawFeatures")
            .setOutputCol("features")

        // IDF是一个Estimator，调用fit()方法并将词频向量传入，即产生一个IDFModel
        val idfModel=idf.fit(featurizedData)

        // 应该使用测试集测试模型，就不多赘述了
        val result=idfModel.transform(featurizedData)

        // 特征向量已经被其在语料库中出现的总次数进行了修正，通过TF-IDF得到的特征向量，在接下来可以被应用到相关的机器学习方法中
        result.select("features","label").show(100,false)

        spark.stop()
    }

}