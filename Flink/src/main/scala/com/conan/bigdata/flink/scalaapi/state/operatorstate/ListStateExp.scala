package com.conan.bigdata.flink.scalaapi.state.operatorstate

import java.util
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext, StateBackend}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._

/**
  * OperatorState只有一个ListState结构
  */
object ListStateExp {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        env.enableCheckpointing(1000)
        env.setStateBackend((new FsStateBackend("file:///Users/mw/temp/flink/checkpoint/liststate")).asInstanceOf[StateBackend])
        env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
        env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000))

        // 添加自定义source
        env.addSource(new MySource).print()

        env.execute("aaa")
    }
}

// 自定义source，operator state需要实现CheckpointedFunction接口
class MySource extends SourceFunction[String] with CheckpointedFunction {

    var flag = true
    var offset = 0L
    // 定义ListState成员变量
    var state: ListState[Long] = _

    override def run(ctx: SourceContext[String]) = {
        while (flag) {
            // 需要从state中获取之前保存的offset，这里只有一个值，简单演示
            val iterator: util.Iterator[Long] = state.get().iterator()
            // Iterator的迭代一定要使用hasNext在使用next，不可以直接使用next
            while (iterator.hasNext) {
                offset = iterator.next()
            }
            offset += 1
            ctx.collect(offset + "")
            println(s"offset=${offset}")
            TimeUnit.SECONDS.sleep(1)

            // 设置异常，让程序出现异常之后重启，看看是否会从ListState中恢复offset
            if (offset % 5 == 0) {
                println("程序异常...程序重启")
                throw new RuntimeException("程序bug!!!")
            }
        }
    }

    override def cancel() = {
        flag = false
    }

    // 初始化state方法
    override def initializeState(context: FunctionInitializationContext) = {
        // 定义一个ListState的描述器，存储的就是offset值
        val descriptor: ListStateDescriptor[Long] = new ListStateDescriptor[Long]("listState", classOf[Long])
        // 通过context获取ListState
        state = context.getOperatorStateStore.getListState(descriptor)
    }

    // 进行checkpoint时制作state快照，调用此方法
    override def snapshotState(context: FunctionSnapshotContext) = {
        state.clear()
        state.add(offset)
    }
}
