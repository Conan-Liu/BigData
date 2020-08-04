package com.conan.bigdata.spark.streaming.mwee.wx

import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.slf4j.{Logger, LoggerFactory}

class WxAppListener(private val appName: String) extends StreamingListener {

    private val log: Logger = LoggerFactory.getLogger("WxAppListener")

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {

        val batchInfo = batchCompleted.batchInfo
        val batchTime = batchInfo.batchTime.milliseconds
        val processingStartTime = batchInfo.processingStartTime
        val processingEndTime = batchInfo.processingEndTime
        val numRecords = batchInfo.numRecords
        val processingDelay = batchInfo.processingDelay
        val totalDelay = batchInfo.totalDelay


        if (numRecords > 0) {
            val logContent = s"App[${appName}]: batchTime -> ${batchTime}, numRecords -> ${numRecords}, processingDelay -> ${processingDelay}, "
            log.warn(logContent)
            // 启用微信告警，需要考虑一个好的方案来启用，否则告警会很频繁
            val wxBody = "{\"type\":\"2\",\"receiverMobiles\":\"13852293070\",\"subject\":\"测试 subject\",\"content\":\"" + logContent + "\"}\"}"
            Tools.doPost("http://alarm-notify.mwbyd.cn/services/notify/pushAll", wxBody)
        }
    }
}
