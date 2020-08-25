package com.conan.bigdata.flink.scalaapi.streaming

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
  * 八股文编程
  * 1. 创建一个execution environment
  * 2. 创建加载源数据Source
  * 3. 对数据进行Transformation（这一步可以没有）
  * 4. 指定计算结果的输出Sink
  * 5. 触发程序执行
  *
  * WIN 环境
  * nc -lL -p 9999
  */
object NetworkWordCount {

    def main(args: Array[String]): Unit = {
        // Flink自带工具类获取命令行参数
        val tool = ParameterTool.fromArgs(args)
        val host = tool.get("host", "localhost")
        val port = tool.getInt("port", 9999)

        // 流处理程序，需要创建StreamExecutionEnvironment来处理，相当于Context
        val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        //env.enableCheckpointing(2000)
        //env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

        val text:DataStream[String] = env.socketTextStream(host, port)
        // 字符串转换
        val wordsDS = text.flatMap(_.toLowerCase.split("\\s+")).map((_, 1))
        // 对数据分组，keyBy()可以支持多个字段汇总，也就是说和sql一样groupby多个字段，不需要再像spark样，把所有groupby的字段封装成一个k
        val keyByDS: KeyedStream[(String, Int), Tuple] = wordsDS.keyBy(0)
        // 划分窗口
        val windowDS: WindowedStream[(String, Int), Tuple, TimeWindow] = keyByDS.timeWindow(Time.seconds(5))
        val sumDS: DataStream[(String, Int)] = windowDS.sum(1)
        // 打印
        sumDS.print()
        // 流处理必须指定该方法，来启动任务，就算使用print()方法，也需要指定，和批处理有些不同
        env.execute(getClass.getName)
    }
}