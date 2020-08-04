package com.conan.bigdata.spark.streaming.mwee.wx

import com.alibaba.fastjson.JSON
import org.apache.hadoop.hbase.TableName
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Test {

    def main(args: Array[String]): Unit = {
        val batchDuration = Seconds(20)
        val sparkConf = new SparkConf()
        sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "100")
        val sc = new SparkContext(sparkConf)
        sc.setLogLevel("WARN")
        val ssc = new StreamingContext(sc, batchDuration)

        val properties = Tools.properties
        val kafkaParams = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> properties.getProperty("kafka.broker.list"),
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
            ConsumerConfig.GROUP_ID_CONFIG -> properties.getProperty("kafka.consumer.groupid"),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest", // latest   earliest
            // java 数据类型
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
        )

        val topics = Set(properties.getProperty("kafka.topic"))

        val sourceStream = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            // 这里offset没有存储在第三方，就没使用assign模式，直接使用subscribe模式
            ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
        )

        sourceStream.map(x => JSON.parseObject(x.value())).foreachRDD(rdd => {
            rdd.foreachPartition(p => {
                val conn = HbaseUtils.getHbaseConnection
                val tableName=TableName.valueOf(Constant.HBASE_TABLE_NAME)
                val table = conn.getTable(tableName)
                println(s"=> ${conn.isClosed}, ${table.getName.getNameAsString},${conn.getAdmin.isTableEnabled(tableName)}")
                p.foreach(x => {
                    val city = HbaseUtils.wxGetCity(table, x, 2)
                    println(s"=> ${city}, ${x.toJSONString}")
                })
            })
        })

        ssc.start()
        ssc.awaitTermination()
    }
}
