package com.conan.bigdata.flink.scalaapi.streaming

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Watermark + EventTime + Allowed Lateness + SideOutput确保数据不丢失
  * Watermark窗口延迟计算
  * Allowed Lateness窗口延迟关闭，迟到的数据可以继续被计算，这是在水印基础上继续增加的延迟，两者有区别要注意
  * SideOutput处理以上延迟超过以上两种机制的数据，不再参与计算，只做为保存数据
  */
object WatermarkExp2 {

    case class CarWc(id: String, num: Int, ts: Long)

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 设置时间为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        // 设置watermark的时间周期，默认200ms
        env.getConfig.setAutoWatermarkInterval(200)
        env.setParallelism(1)

        // 添加socket source
        val textStream: DataStream[String] = env.socketTextStream("localhost", 9999)

        // 数据处理后添加watermark，周期性watermark
        val carDS = textStream.map(line => {
            val split = line.split(",")
            CarWc(split(0), split(1).trim.toInt, split(2).trim.toLong)
        })
        val watermarkDS = carDS.assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor[CarWc](Time.seconds(2)) {
                // 这个方法用来告诉Flink，数据中哪个字段是eventTime
                override def extractTimestamp(element: CarWc): Long = {
                    element.ts
                }
            })


        val tag: OutputTag[CarWc] = new OutputTag[CarWc]("lateCarWc")
        val windowDS = watermarkDS
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                // 设置窗口的允许延迟关闭时间
                .allowedLateness(Time.seconds(5))
                // 设置旁路侧道输出
                .sideOutputLateData(tag)

        val resultDS = windowDS.apply(new WindowFunction[CarWc, CarWc, Tuple, TimeWindow] {
            override def apply(key: Tuple, window: TimeWindow, input: Iterable[CarWc], out: Collector[CarWc]): Unit = {
                println(s"窗口开始时间：${window.getStart}, 结束时间${window.getEnd}...")
                println(s"窗口中数据${input.mkString(",")}")
                val wc = input.reduce((c1, c2) => {
                    CarWc(c1.id, c1.num + c2.num, c2.ts)
                })
                out.collect(wc)
            }
        })

        resultDS.print()

        // 获取旁路侧道输出
        val output: DataStream[CarWc] = resultDS.getSideOutput(tag)
        output.print()

        env.execute("aaa")
    }
}
