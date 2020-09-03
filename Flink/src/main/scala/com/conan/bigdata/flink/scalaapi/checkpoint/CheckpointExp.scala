package com.conan.bigdata.flink.scalaapi.checkpoint

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * 演示Checkpoint
  */
object CheckpointExp {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 设置checkpoint属性
        env.setStateBackend(new FsStateBackend(""))
        // 设置checkpoint的时间周期，默认没有开启，需要手动指定
        env.enableCheckpointing(1000)
        // 设置checkpoint的执行语义，选择精确一次
        env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
        // 设置两次checkpoint的最小间隔，因为checkpoint可能会花费一点时间，前面设置1000ms，假设执行了800ms，如果不设置最小时间间隔，那么过200ms后又会执行，浪费性能
        env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
        // 设置checkpoint超时时间，超过设置时间，认为本次执行失败，继续下一次即可
        env.getCheckpointConfig.setCheckpointTimeout(60000)
        // 设置checkpoint出现问题的话，是否让程序报错还是继续执行下一个checkpoint，true报错，false继续下次checkpoint
        env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    }
}
