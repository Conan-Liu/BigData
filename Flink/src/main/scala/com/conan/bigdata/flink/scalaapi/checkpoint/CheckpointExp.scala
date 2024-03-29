package com.conan.bigdata.flink.scalaapi.checkpoint

import java.util.concurrent.TimeUnit

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._

/**
  * 演示Checkpoint
  */
object CheckpointExp {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 设置checkpoint属性
        env.setStateBackend(new FsStateBackend("file:///Users/mw/temp/flink/checkpoint/checkpointexp"))
        // 设置checkpoint的时间周期，默认没有开启，需要手动指定
        env.enableCheckpointing(1000)
        val ckConfig: CheckpointConfig = env.getCheckpointConfig
        // 设置checkpoint的执行语义，选择精确一次
        ckConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
        // 设置两次checkpoint的最小间隔，因为checkpoint可能会花费一点时间，前面设置1000ms，假设执行了800ms，如果不设置最小时间间隔，那么过200ms后又会执行，浪费性能
        ckConfig.setMinPauseBetweenCheckpoints(500)
        // 设置checkpoint超时时间，超过设置时间，认为本次执行失败，继续下一次即可
        ckConfig.setCheckpointTimeout(60000)
        // 设置checkpoint出现问题的话，是否让程序报错还是继续执行下一个checkpoint，true报错，false继续下次checkpoint
        ckConfig.setFailOnCheckpointingErrors(false)
        // 设置任务取消时是否保留检查点，retain则保存检查点数据，delete则删除checkpoint作业数据
        ckConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
        // 设置程序中同时允许几个checkpoint任务进行
        ckConfig.setMaxConcurrentCheckpoints(1)

        // 自定义Source
        val source = env.addSource(
            new RichSourceFunction[String] {
                var flag = true

                override def cancel(): Unit = {
                    flag = false
                }

                override def run(ctx: SourceContext[String]): Unit = {
                    while (flag) {
                        ctx.collect("hello flink")
                        TimeUnit.SECONDS.sleep(1)
                    }
                }
            }
        )

        source.print()
        env.execute("aaa")
    }
}
