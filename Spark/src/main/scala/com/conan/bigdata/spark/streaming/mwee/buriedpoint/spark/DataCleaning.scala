package com.conan.bigdata.spark.streaming.mwee.buriedpoint.spark

import com.conan.bigdata.spark.streaming.mwee.buriedpoint.service.CleaningService
import kafka.serializer.StringDecoder
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.CanCommitOffsets
import org.apache.spark.streaming.{Minutes, StreamingContext}
import org.slf4j.LoggerFactory

/**
  * Created by Conan on 2019/5/5.
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

        val streamD = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, null, null)

        // 数据清洗
        streamD.foreachRDD(rdd => {
            val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            try {
                rdd.filter(message => message != null && StringUtils.isNotBlank(message._2))
                    .map(_._2)
                    .foreachPartition(partition => {
                        partition.foreach(record => {
                            CleaningService.transformAndSave(record)
                        })
                    })

                // 遍历完 partition ，最后要提交offset， 每个partition都要提交
                // 注意下面这个代码是错误的， 我本地用的kafka是0.8的，没有这个方法
                // 只有 0.10 的才有这个方法， 留作修改
                // TODO ... Streaming整合kafka的版本更新
                //                streamD.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
            } catch {
                case e: Exception => LOG.error(ExceptionUtils.getStackTrace(e))
            }
        })

        ssc.start()
        ssc.awaitTermination()
    }
}