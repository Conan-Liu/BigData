package com.conan.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, State, StreamingContext}

/**
  * 无状态的：当前批次处理完之后，数据只与当前批次有关
  * 有状态的：前后批次的数据处理完之后，之间是有关系的
  *
  * 该类，记录了从运行时刻， 单词统计的历史记录， 计数， 是从最开始， 一直往后累计的功能
  * 累计的状态checkpoint到了指定目录
  * 状态统计
  */
object StateWordCount {

    val host = "192.168.1.8"
    val port = 9999

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setAppName("UpdateStateWordCount").setMaster("local[2]")
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        ssc.sparkContext.setLogLevel("warn")

        // 使用updateStateByKey 算子， 一定要使用checkpoint保存下来
        // 生产环境上， 建议checkpoint到HDFS上， 这个例子保存到当前路径
        // 如果Streaming程序的代码改变了，重新打包执行就会出现反序列s化异常的问题
        // 这里如果window环境，需要winutils.exe，否则NPE
        ssc.checkpoint("/user/root/temp/spark/checkpoint/statewordcount")

        val lines = ssc.socketTextStream(host, port)
        val result = lines.flatMap(x => x.split("\\s+")).map(x => (x, 1))
        //
        //        val state1 = result.mapWithState(StateSpec.function(mapState _).timeout(Seconds(30)))

        val state = result.updateStateByKey[Int]((x, y) => updateState(x, y))
        state.print

        ssc.start
        ssc.awaitTermination
    }


    def updateState(currentValues: Seq[Int], stateValues: Option[Int]): Option[Int] = {
        val current = currentValues.sum
        val pre = stateValues.getOrElse(0)

        Option(current + pre)
    }

    /**
      * 下面演示 mapWithState方法使用
      */
    def mapState(word: String, option: Option[Int], state: State[Int]): (String, Int) = {
        if (state.isTimingOut()) {
            println(s"${word} is timeout")
            (word, 0)
        } else {
            // 获取当前值 + 历史值
            val sum = option.getOrElse(0) + state.getOption().getOrElse(0)
            // 更新状态
            state.update(sum)
            (word, sum)
        }
    }

}