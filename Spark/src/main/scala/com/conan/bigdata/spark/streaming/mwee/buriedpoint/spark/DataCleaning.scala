package com.conan.bigdata.spark.streaming.mwee.buriedpoint.spark

import com.conan.bigdata.spark.streaming.mwee.buriedpoint.service.CleaningService
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils
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
        val ssc = new StreamingContext(conf, Minutes(1))

        var kafkaParams: Map[String, Object] = Map()
        kafkaParams += ("bootstrap.servers" -> "CentOS:9092", )

        val streamD = KafkaUtils.createDirectStream[String, String](
            ssc,
            // 位置策略（可用的Executor上均匀分配分区）
            LocationStrategies.PreferConsistent,
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
                // TODO ... Streaming整合kafka的版本更新
                streamD.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
            } catch {
                case e: Exception => LOG.error(ExceptionUtils.getStackTrace(e))
            }
        })

        ssc.start()
        ssc.awaitTermination()
    }
}