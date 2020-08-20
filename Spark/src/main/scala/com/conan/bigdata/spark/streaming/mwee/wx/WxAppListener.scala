package com.conan.bigdata.spark.streaming.mwee.wx

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}

class WxAppListener(private val appName: String) extends StreamingListener with Logging {

    @volatile private var count = 0

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {

        val batchInfo = batchCompleted.batchInfo
        val batchTime = batchInfo.batchTime.milliseconds
        val processingStartTime = batchInfo.processingStartTime
        // 任务是否延迟执行
        val schedulingDelay = batchInfo.schedulingDelay.getOrElse(0L)
        val processingEndTime = batchInfo.processingEndTime
        val numRecords = batchInfo.numRecords
        val processingDelay = batchInfo.processingDelay
        val totalDelay = batchInfo.totalDelay


        //val logContent = s"App[${appName}]: batchTime -> ${batchTime}, numRecords -> ${numRecords}, processingDelay -> ${processingDelay}, "
        var wxBody: String = null
        if (numRecords <= 0) {
            count = count + 1
            if (count >= 3) {
                wxBody = "{\"type\":\"2\",\"receiverMobiles\":\"13852293070\",\"subject\":\"测试 subject\",\"content\":\"已经三个batch没有获取数据，请检查数据源\"}\"}"
                count = 0
            }
        } else {
            // 监控batch积压
        }
        Tools.doPost("http://alarm-notify.mwbyd.cn/services/notify/pushAll", wxBody)
    }
}
