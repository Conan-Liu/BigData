package com.conan.bigdata.flink.scalaapi.streaming

import org.apache.flink.api.common.functions.{AggregateFunction, FoldFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.windowing.time.Time

// org.apache.flink.streaming.api.scala._
// 引入这个包可以解决 could not find implicit TypeInformation 的错误
// 用于处理Stream
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.wikiedits.{WikipediaEditEvent, WikipediaEditsSource}

/**
  * 官方封装的监控Wikipedia实时编辑情况
  */
object WikipediaAnalysis {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 添加Source
        // 这里有个隐式转换，可以自己定义，如下，这种写法显得过于麻烦，每一个类型都要自己转换一次，所以推荐直接把包引进来即可
        // implicit val typeInfo1=TypeInformation.of(classOf[WikipediaEditEvent])
        val edit: DataStream[WikipediaEditEvent] = env.addSource(new WikipediaEditsSource())

        // 业务处理逻辑
        // implicit val typeInfo2=TypeInformation.of[String](classOf[String])
        // 按照 event.getUser 的值来分区
        val keyStream = edit.keyBy(new KeySelector[WikipediaEditEvent, String] {
            override def getKey(event: WikipediaEditEvent): String = event.getUser
        })

        //添加Sink
        // fold 函数不在推荐使用，aggregate 代替
        val result1 = keyStream.timeWindow(Time.seconds(10))
            // 柯里化函数
            .fold(("", 0))((x, y) => {
            (x._1, x._2)
        })
        val result2 = keyStream.timeWindow(Time.seconds(10))
            .fold(("", 0), new FoldFunction[WikipediaEditEvent, (String, Int)] {
                override def fold(accumulator: (String, Int), value: WikipediaEditEvent): (String, Int) = {
                    (value.getUser, accumulator._2 + value.getByteDiff)
                }
            })
        val result3 = keyStream.timeWindow(Time.seconds(10))
            .aggregate(new AggregateFunction[WikipediaEditEvent, (String, Int), (String, Int)] {
                override def createAccumulator(): (String, Int) = ("", 0)

                override def merge(a: (String, Int), b: (String, Int)): (String, Int) = (b._1,a._2+b._2)

                override def getResult(accumulator: (String, Int)): (String, Int) = accumulator

                override def add(value: WikipediaEditEvent, accumulator: (String, Int)): (String, Int) =(value.getUser,accumulator._2+value.getByteDiff)
            })
        result3.print()
        env.execute()
    }
}