package com.conan.bigdata.spark.streaming

import com.conan.bigdata.spark.utils.Spark
import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Conan on 2019/4/28.
  * SparkStreaming 整合 Flume
  * flume 监控控制台输入， 然后通过sink到指定avro端口， sparkstreaming来监听该端口
  * 这是 flume 推送数据到Streaming  Flume-style Push-based Approach
  * 对应 flume_push_streaming.conf
  */
object FlumePushWordCount {
    def main(args: Array[String]): Unit = {

        if (args.length != 2) {
            println("Need Two Parameters")
            System.exit(1)
        }

        val Array(hostname, port) = args

        //        val sparkConf = new SparkConf().setAppName("FlumePushWordCount").setMaster("local[2]")
        val sparkConf = new SparkConf() // 提交到集群上运行， 就不要指定name和master， submit是再指定
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        ssc.sparkContext.setLogLevel("WARN")

        // 直接调用工具类即可
        // 这里sparkstreaming的设置成本地启动， worker 节点要和flume监听的程序 在一台机器上
        // 而且， 本地的网络输入数据可以使用 nc -lk 41414 , 它会占用41414端口，来传输数据
        // 如果一个端口已经被占用， 接收外部输入的数据， 可以使用 telnet CentOS 41414
        // 这两个命令的区别就是， 这个端口是别人占用的话，就用telent， 自己占用就用 netcat
        val flumeStream = FlumeUtils.createStream(ssc, hostname, port.toInt)
        // flume 发过来的基本数据单元是Event， 里面包含body和header， 所以需要多一步解析， 如map
        val result = flumeStream.map(x => new String(x.event.getBody.array()).trim)
            .flatMap(_.split("\\s+")).map(x => (x, 1)).reduceByKey(_ + _)
        result.print()

        ssc.start()
        ssc.awaitTermination()
    }


}