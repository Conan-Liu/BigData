package com.conan.bigdata.spark.streaming.flownotice

import com.conan.bigdata.spark.streaming.flownotice.util.{KafkaClient, RedisClient}
import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  */
object StreamingAPP {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("StreamingAPP").setMaster("local[2]")
        val ssc = new StreamingContext(sparkConf, Seconds(10))
        ssc.sparkContext.setLogLevel("WARN")

        val bcRedisClient = ssc.sparkContext.broadcast(RedisClient)
        val bcKafkaClient = ssc.sparkContext.broadcast(KafkaClient)

        val Array(broker, topics) = Array("CentOS:9092", "kafkastreaming")

        var kafkaParams = Map[String, String]()
        kafkaParams += ("bootstrap.servers" -> broker)

        val topicSet = topics.split(",").toSet

        val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)
        // 这里getPhoneDateFlow需要一个参数， 但是map里面不写， map函数本身就是遍历每一个元素， 所以自动把每一个元素作为参数传递给getPhoneDateFlow
        val flowInfo = stream.map(_._2).map(getPhoneDateFlow).reduceByKey(_ + _).map(x => (x._1._1, x._1._2, x._2))

        flowInfo.print()

        flowInfo.foreachRDD(rdd => {
            // redis 和 kafka的操作， 用广播变量比较好
            rdd.foreachPartition(partition => {
                val redisClient = bcRedisClient.value
                redisClient.hincrBy(partition.toList)
                bcKafkaClient.value.sendMessage("partitions: " + TaskContext.getPartitionId())
            })
        })

        ssc.start()
        ssc.awaitTermination()
    }

    def getPhoneDateFlow(x: String): ((String, String), Float) = {
        try {
            val strs = x.split("\\s+")
            ((strs(1), strs(0)), strs(3).toFloat)
        } catch {
            case e: Exception => println("get flow failure...." + e.getMessage)
                null;
        }
    }
}