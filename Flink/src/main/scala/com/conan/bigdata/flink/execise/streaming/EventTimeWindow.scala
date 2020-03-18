package com.conan.bigdata.flink.execise.streaming

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object EventTimeWindow {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 设置flink的时间机制是EventTime，默认ProcessingTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val host = "192.168.1.8"
        val port = 9999
        // 这段代码不能触发执行
        val stream = env.socketTextStream(host, port).map(_.split("\\s+")).assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor[Array[String]](Time.milliseconds(0)) {
                override def extractTimestamp(element: Array[String]): Long = {
                    element(0).toLong
                }
            }
        )
        // 这个正常， 不知道这两段代码有什么差别
        val stream1 = env.socketTextStream(host, port).assignTimestampsAndWatermarks(
            // watermark 是用来延迟执行的，这里表示延迟1s，有时候EventTime不能准时到达，可以用这个来等一等晚到的数据
            new BoundedOutOfOrdernessTimestampExtractor[String](Time.milliseconds(1000)) {
                override def extractTimestamp(element: String): Long = {
                    val ss = element.split("\\s+")
                    println(ss(0))
                    ss(0).toLong
                }
            }
        )
        // val result = stream.map(x => (x(1), 1))
        val result = stream1.map(_.split("\\s+")).map(x => (x(1), 1))
            .keyBy(0)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce((x, y) => (x._1, x._2 + y._2))

        result.print()

        env.execute("EventTimeWindow")
    }
}