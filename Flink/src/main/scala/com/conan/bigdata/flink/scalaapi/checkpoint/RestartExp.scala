package com.conan.bigdata.flink.scalaapi.checkpoint

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._

/**
  * Flink Restart重启策略演示
  */
object RestartExp {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 全局取消operator chain机制
        env.disableOperatorChaining()
        // 设置checkpoint属性
        env.setStateBackend(new FsStateBackend("file:///Users/mw/temp/flink/checkpoint/checkpointexp"))
        // 设置checkpoint的时间周期，默认没有开启，需要手动指定
        env.enableCheckpointing(1000)
        val ckConfig: CheckpointConfig = env.getCheckpointConfig
        ckConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
        // 固定重启策略，重启3次，重启间隔5秒
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,Time.of(5,TimeUnit.SECONDS)))

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
