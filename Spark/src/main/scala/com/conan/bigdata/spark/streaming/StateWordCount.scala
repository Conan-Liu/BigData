package com.conan.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

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

        val sparkConf = new SparkConf().setAppName("UpdateStateWordCount")
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        ssc.sparkContext.setLogLevel("warn")

        // 使用updateStateByKey 算子， 一定要使用checkpoint保存下来
        // 生产环境上， 建议checkpoint到HDFS上， 这个例子保存到当前路径
        // 如果Streaming程序的代码改变了，重新打包执行就会出现反序列s化异常的问题
        // 这里如果window环境，需要winutils.exe，否则NPE
        ssc.checkpoint("/user/root/temp/spark/checkpoint/statewordcount")

        val lines = ssc.socketTextStream(host, port)
        val result = lines.flatMap(x => x.split("\\s+")).map(x => (x, 1))

        // 假设一个RDD之前的依赖很长，计算很耗时，如果任务出错，又得根据血缘关系从头恢复，耗时耗资源
        // cache和persist都可以打断依赖，RDD缓存在内存中，但是依旧存在内存缓存数据丢失的风险
        // 那么就可以使用RDD.checkpoint来把RDD保存到hdfs上，checkpoint
        // result.cache().checkpoint(这个参数还不理解)


        // 比updateStateByKey保存更多的Key，更低的延时
        // mapStateFun _  表示方法转函数， 注意函数是继承自Function类，而方法是每个类几乎都有的代码块
        // 从界面打印结果来看，是增量的形式累加
        // 这里设置了过期时间，如果超过了这个时间，该Key一直没有更新，value就会就被重置为0，如果有更新的话，就会重置计时
        val mapState = result.mapWithState(StateSpec.function(mapStateFun _).timeout(Seconds(30)))
        //  mapState.print

        // 这个带状态的算子，会随着程序从启动开始计算，一直运行下去
        // 这是个全局的概念，无论Key有没有更新，都会执行一遍更新操作，效率低，所以不大适用
        val state = result.updateStateByKey[Int]((x, y) => updateState(x, y))
        //  state.print

        // 常用的是Window算子，可以限定时间段
        val windowWordCount = result.window(Seconds(600), Seconds(20)).reduceByKey(_ + _)
        windowWordCount.print

        ssc.start
        ssc.awaitTermination
    }


    def updateState(currentValues: Seq[Int], stateValues: Option[Int]): Option[Int] = {
        val current = currentValues.sum
        val pre = stateValues.getOrElse(0)
        // 返回None的情况下，会删除状态里面对应的Key，这里演示的是这个Batch如果单个单词总数超过5个，就删除
        if (current >= 5)
            None
        else
            Option(current + pre)
    }

    /**
      * 下面演示 mapWithState方法使用
      */
    def mapStateFun(word: String, option: Option[Int], state: State[Int]): (String, Int) = {
        if (state.isTimingOut()) {
            println(s"${word} is timeout")
            // value重置为0
            (word, 0)
        } else {
            // 获取 历史值 + 当前值
            val sum = option.getOrElse(0) + state.getOption().getOrElse(0)
            // 更新状态
            state.update(sum)
            (word, sum)
        }
    }

}