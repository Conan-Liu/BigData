package com.conan.bigdata.flink.execise.streaming

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ConnectSplitStream {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val data1 = env.generateSequence(1, 10)
        val data2 = env.fromCollection(Seq[(Long, String)]((1, "hadoop"), (2, "spark"), (3, "flink"), (4, "hive"), (5, "hbase")))

        println("*****************connect************************")
        // 注意ConnectedStreams类除了map flatmap keyby几乎不包含其它方法，所以connect后面一定会跟map或flatmap
        // connect方法可以连接两个不同类型的DataStream
        val connectStream = data1.connect(data2).map(x1 => (x1, "liu"), x2 => x2)
        // 下面这个执行不通过
        // val connectStream1 = data1.connect(data2).map((_, "liu"), _)
        //        connectStream.print().setParallelism(2)

        println("*****************union************************")
        // union 只能连接两个相同类型的DataStream
        val unionStream = data1.union(data2.map(_._1))
        //        unionStream.print().setParallelism(2)

        println("*****************split************************")
        /**
          * flink里的split已经不推荐使用，官方推荐旁路输出来代替
          * spark里只能通过filter来实现拆分一个流，如果是多个流，需要多个filter，不灵活，flink提供side out put旁路输出，一次拆分多个流
          * 侧输出结果流的数据类型不需要与主数据流的类型一致，不同侧输出流的类型也可以不同
          * 注意数据是直接判定输出到哪个流，不会复制一份，也就是说一条数据只能有一条去路
          */
        // 单旁路输出，这种的话，感觉直接filter就行了
        val outputTag = OutputTag[String]("side-output")
        val mainDataStream = data2.map(_._1).process(new ProcessFunction[Long, Long] {
            override def processElement(value: Long, ctx: ProcessFunction[Long, Long]#Context, out: Collector[Long]): Unit = {
                out.collect(value)
                ctx.output(outputTag, "sideout-" + value)
            }
        })
        val sideOutputStream = mainDataStream.getSideOutput(outputTag)
        //        sideOutputStream.print().setParallelism(2)

        /**
          * 多旁路输出，能提升分流的效率
          * 如下原来的流是 data2，拆分成了三个新的流
          * data2 = multiDataStream + outputTag1 + outputTag2
          * 其中outputTag1 2是由用户自定义流， 表示旁路输出的流，由Context收集
          * multiDataStream是process的返回值，表示旁路输出完成后还剩下来的数据组成的流，由Collector收集
          */
        val outputTag1 = OutputTag[(Long, String)]("outputTag1")
        val outputTag2 = OutputTag[(Long, String)]("outputTag2")
        val multiDataStream = data2.process(new ProcessFunction[(Long, String), (Long, String)] {
            override def processElement(value: (Long, String), ctx: ProcessFunction[(Long, String), (Long, String)]#Context, out: Collector[(Long, String)]): Unit = {
                if (value._2 == "hadoop") {
                    ctx.output(outputTag1, (value._1, value._2 + "-1"))
                } else if (value._2 == "spark" || value._2 == "flink") {
                    ctx.output(outputTag2, (value._1, value._2 + "-2"))
                } else {
                    out.collect((value._1, value._2 + "-old"))
                }
            }
        })
        // 自定义的旁路输出流
        multiDataStream.getSideOutput(outputTag1).print().setParallelism(1)
        multiDataStream.getSideOutput(outputTag2).print().setParallelism(1)
        // 这个表示原来流里面的数据经过process旁路输出完成后还剩下来的数据组成的流，由Collector来收集
        multiDataStream.print().setParallelism(1)


        // 延迟执行导致，前面三个打印成了摆设，并不是按顺序打印
        env.execute("ConnectSplitStream")

    }

}