package com.conan.bigdata.spark.streaming

import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}

/**
  * mac 机器
  * nc -l 9999
  * 接收 9999 端口传来的数据， 并处理
  */
object App {

    private val log: Logger = LoggerFactory.getLogger("App")

    def main(args: Array[String]) {
        //设置日志输出级别，省的控制台全是没用的日志
        org.apache.log4j.Logger.getLogger("java.lang").setLevel(org.apache.log4j.Level.OFF)
        org.apache.log4j.Logger.getLogger("org.spark_project").setLevel(org.apache.log4j.Level.OFF)
        org.apache.log4j.Logger.getLogger("io.netty").setLevel(org.apache.log4j.Level.OFF)
        org.apache.log4j.Logger.getLogger("org.apache.hadoop").setLevel(org.apache.log4j.Level.OFF)
        org.apache.log4j.Logger.getLogger("org.apache.spark").setLevel(org.apache.log4j.Level.OFF)

        val conf = new SparkConf().setAppName("App").setMaster("local[2]")
        val ssc = new StreamingContext(conf, Seconds(20))

        val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)

        // TODO... 这里多线程如果调用ssc.stop方法，考虑下优雅的关闭
        val t = new Thread(new Runnable {
            override def run(): Unit = {
                var i = 0
                while (true) {
                    i = i + 1
                    println(s"time = ${new Date}")
                    Thread.sleep(4000)
//                    if (i >= 10)
//                        stop()
                }
            }
        }, "tttttt")
        t.start()

        lines.count().print()


        ssc.start()

        // ssc.awaitTermination()
        def stop(): Unit = {
            ssc.stop(true: Boolean, true: Boolean)
        }
    }
}
