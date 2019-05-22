package com.conan.bigdata.spark.streaming.mwee.buriedpoint.spark

import com.conan.bigdata.spark.streaming.mwee.buriedpoint.service.CleaningService
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Minutes, StreamingContext}
import org.slf4j.LoggerFactory

/**
  * Created by Conan on 2019/5/5.
  *
  * 这个数据流程 kafka -> spark streaming -> kafka -> flume -> hdfs
  * 我可能更偏爱 spark streaming 直接到 hdfs
  */
object DataCleaning {

    private val LOG = LoggerFactory.getLogger(this.getClass)

    def main(args: Array[String]): Unit = {
        if (args.length == 0) {
            System.exit(1)
        }
        val conf = new SparkConf() //.setAppName("DataCleaning").setMaster("")
            .set("spark.streaming.stopGracefullyOnShutdown", "true")
            .set("spark.streaming.kafka.maxRatePerPartition", "") // 反压 控制刚开始启动的时候， 要消费的数据量过大造成压力
        val ssc = new StreamingContext(conf, Minutes(1))

        val kafkaParams: Map[String, Object] = Map(
            "bootstrap.servers" -> "CentOS:9092",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
            "group.id" -> "wahaha",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
            "enable.auto.commit" -> false //关闭自动提交
        )

        val streamD = KafkaUtils.createDirectStream[String, String](
            ssc,
            // 位置策略（可用的Executor上均匀分配分区）
            LocationStrategies.PreferConsistent,
            // 需要指定offset 比较完善
            ConsumerStrategies.Subscribe[String, String](Array("topic1"), kafkaParams)
        )

        // 数据清洗
        streamD.foreachRDD(rdd => {
            // 因为这里没有其它转换， 直接读取的源DStream， 可以直接通过这种方式访问OffsetRanges
            // 如果有计算转换， 那么可以先通过源DStream调用transform来获取保存到变量后， 在进行计算逻辑
            val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            try {
                rdd.filter(message => message != null && StringUtils.isNotBlank(message.value()))
                    .map(_.value())
                    .foreachPartition(partition => {
                        partition.foreach(record => {
                            CleaningService.transformAndSave(record)
                        })
                    })

                // 遍历完 partition ，最后要提交offset， 每个partition都要提交
                // 注意下面这个代码是错误的， 我本地用的kafka是0.8的，没有这个方法
                // 只有 0.10 的才有这个方法， 留作修改
                // 这个方法会异步提交offset到指定的commitQueue里， 等下一个批次计算的时候才会把这个提交上去
                // 这样如果停止后， 其实最后一个消费的批次， offset并不会成功的提交， 导致下次启动会重复消费
                streamD.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
            } catch {
                case e: Exception => LOG.error(ExceptionUtils.getStackTrace(e))
            }
        })

        ssc.start()
        ssc.awaitTermination()
    }
}