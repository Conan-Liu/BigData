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
  * Watermark + EventTime处理数据延迟到达的问题
  * 但是只能延迟一段时间，无法解决长时间的延迟，也无法解决数据丢失问题
  * 窗口延迟计算，触发计算后，窗口立即销毁
  */
object WatermarkExp {

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

        // 自定义watermark方法，平时只需要使用已经提供的即可，无需额外自定义实现
        carDS.assignTimestampsAndWatermarks(
            new AssignerWithPeriodicWatermarks[CarWc] {
                // 定义watermark的延迟时间
                val delayTime = 2000
                var eventTime = 0L
                // 定义最大的时间戳
                var currentMaxTime:Long = 0

                override def getCurrentWatermark: Watermark = {
                    val watermark = currentMaxTime - delayTime
                    new Watermark(watermark)
                }

                // element 数据元素，previousElementTimestamp 之前元素的最大时间戳
                override def extractTimestamp(element: CarWc, previousElementTimestamp: Long): Long = {
                    eventTime = element.ts
                    if (eventTime > currentMaxTime)
                        currentMaxTime = eventTime
                    eventTime
                }
            })


        val windowDS = watermarkDS
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))

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

        env.execute("aaa")
    }
}
