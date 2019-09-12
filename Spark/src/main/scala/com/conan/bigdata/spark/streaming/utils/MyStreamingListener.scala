package com.conan.bigdata.spark.streaming.utils

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.scheduler._
import org.slf4j.LoggerFactory

/**
  * 这个StreamingListener监控， 主要是用来收集任务执行时的一些数据，
  * 这些数据可以在WebUI上看到， 不过对于开发者来说， 总不至于经常看Web页面，
  * 可以通过这个程序来发送邮件实现batch任务信息的收集
  * 不过这样每个批次都会发送邮件， 那么邮件就会很多， 可以考虑写入数据库
  *
  * 以 yarn-client 模式提交可以调试看到效果
  */
class MyStreamingListener extends StreamingListener {

    val LOG=LoggerFactory.getLogger("StreamingListener")

    // 流式计算开始时，出发的回调函数, 这个是streaming app提交时仅执行一次
    override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit = {
        val startTime = streamingStarted.time
//        println(s"任务启动时间:${startTime}")
        LOG.info(s"任务启动时间:${startTime}")
    }

    // 当前batch提交时出发的回调函数
    //    override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {
    //        batchSubmitted.batchInfo.streamIdToInputInfo.foreach(tuple => {
    //            val streamingInputInfo = tuple._2
    //            streamingInputInfo.metadata
    //            streamingInputInfo.numRecords
    //            streamingInputInfo.inputStreamId
    //            streamingInputInfo.metadataDescription
    //        })
    //
    //        batchSubmitted.batchInfo.numRecords
    //        batchSubmitted.batchInfo.outputOperationInfos
    //        batchSubmitted.batchInfo.submissionTime
    //        batchSubmitted.batchInfo.batchTime
    //        batchSubmitted.batchInfo.processingStartTime
    //        batchSubmitted.batchInfo.processingEndTime
    //        batchSubmitted.batchInfo.processingDelay
    //        batchSubmitted.batchInfo.schedulingDelay
    //        batchSubmitted.batchInfo.totalDelay
    //    }

    // 当前batch完成时触发的回调函数
    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
        val numRecords = batchCompleted.batchInfo.numRecords
        val endTime = batchCompleted.batchInfo.batchTime
        val processingStartTime = batchCompleted.batchInfo.processingStartTime.getOrElse(0l)
        val processingEndTime = batchCompleted.batchInfo.processingEndTime.getOrElse(0l)
        val processingDelay = batchCompleted.batchInfo.processingDelay.getOrElse(0l)
        val schedulingDelay = batchCompleted.batchInfo.schedulingDelay.getOrElse(0l)
        val totalDelay = batchCompleted.batchInfo.totalDelay.getOrElse(0l)
        println(s"1:${numRecords}, 2:${endTime}, 3:${processingDelay}, 4:${totalDelay}")
        // 获取offset，并持久化到第三方容器
        // TODO...
    }

    // 当output开始时触发的回调函数
    //    override def onOutputOperationStarted(outputOperationStarted: StreamingListenerOutputOperationStarted): Unit = {
    //        outputOperationStarted.outputOperationInfo.description
    //        outputOperationStarted.outputOperationInfo.batchTime
    //        outputOperationStarted.outputOperationInfo.endTime
    //        outputOperationStarted.outputOperationInfo.failureReason
    //        outputOperationStarted.outputOperationInfo.id
    //        outputOperationStarted.outputOperationInfo.name
    //        outputOperationStarted.outputOperationInfo.startTime
    //        outputOperationStarted.outputOperationInfo.duration
    //}

    // 当output结束时触发的回调函数
    //    override def onOutputOperationCompleted(outputOperationCompleted: StreamingListenerOutputOperationCompleted): Unit = {
    //        outputOperationCompleted.outputOperationInfo.duration
    //        outputOperationCompleted.outputOperationInfo.startTime
    //        outputOperationCompleted.outputOperationInfo.name
    //        outputOperationCompleted.outputOperationInfo.id
    //        outputOperationCompleted.outputOperationInfo.failureReason
    //        outputOperationCompleted.outputOperationInfo.endTime
    //        outputOperationCompleted.outputOperationInfo.batchTime
    //        outputOperationCompleted.outputOperationInfo.description
    //    }

}