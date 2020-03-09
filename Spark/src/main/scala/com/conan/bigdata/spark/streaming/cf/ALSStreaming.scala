package com.conan.bigdata.spark.streaming.cf

import com.conan.bigdata.spark.utils.SparkVariable
import kafka.serializer.StringDecoder
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 利用ALS离线生成的模型实时给用户推荐物品
  * ALS应用代码参考[[com.conan.bigdata.spark.ml.ALSCF]]
  *
  * Kafka里面的数据是包含user_id的用户访问埋点日志，表示该用户正在浏览页面，应该要给与相应的推荐
  * 注意：
  * 1. 虽然是实时推荐，但是 model 不是实时生成的，也就是说如果是新用户或者新物品，该模型是推荐不了的，因为模型里面不包含新用户和新物品
  * 2. 用户现在浏览一个物品，表示对同类兴趣程度高，但是也许是刚刚感兴趣，模型里面计算的历史数据有可能不涉及到该物品相似度情况，所以也不能合理推荐
  *
  * 流式的推荐系统里，有些物品已经曝光过了，也就是用户访问过了，可以考虑实时的把这些曝光过的数据在推荐的时候过滤掉
  * 综上所述： 这并不是真正的实时推荐，仅仅作为学习
  */
object ALSStreaming extends SparkVariable {

    def main(args: Array[String]): Unit = {
        val ssc = new StreamingContext(sc, Seconds(5))
        val kafkaParams: Map[String, String] = Map(
            // 主要验证推荐效果，这里设置自动提交，减少代码
            "auto.commit.enable" -> "true"
        )
        val topicSet: Set[String] = Set("")
        // 得到
        val kafkaStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)
        // map为了得到流里面过来的user_id
        // 这里注意，流里面的user_id可能模型中不存在，该用户就不会被推荐，代码可以优化，但没必要
        kafkaStream.map(_._2.split(",")(1).toInt).foreachRDD(rdd => {
            rdd.foreachPartition(partition => {
                // 每个Partition单独加载一遍模型，用于推荐
                val model = MatrixFactorizationModel.load(ssc.sparkContext, "")
                partition.foreach(record => {
                    // 可以保存到数据库，供Web查询
                    // Spark提供所有用户和一个用户的推荐，居然不提供批量用户推荐
                    val rmdProducts: Array[Rating] = model.recommendProducts(record, 5)
                })
            })
        })

        ssc.start()
        ssc.awaitTermination()
    }
}