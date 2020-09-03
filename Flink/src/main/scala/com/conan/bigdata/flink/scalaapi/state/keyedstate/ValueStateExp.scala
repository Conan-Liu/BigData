package com.conan.bigdata.flink.scalaapi.state.keyedstate

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
  * [[ValueState]]计算流处理里面的最大值
  */
object ValueStateExp {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val textStream: DataStream[String] = env.socketTextStream("localhost",9999)

        // 使用KeyedStream
        val map: DataStream[(String, Int)] = textStream.map(line => {
            var arr = line.split(",")
            (arr(0), arr(1).trim.toInt)
        })

        // 分组
        val by: KeyedStream[(String, Int), Tuple] = map.keyBy(0)

        // 使用valuestate来存储两两比较之后的最大值，新数据到来之后如果比原来的最大值还大则把该值更新为状态值
        // 保证状态存储的始终是最大值
        // 需要通过RichFunction来获取上下文来操作ValueState
        val maxDS=by.map(new RichMapFunction[(String,Int),(String,Int)] {

            // 需要声明一个ValueState，不能自己创建，通过上下文来获取，这个状态方法，无需关心key，flink自己维护key
            var maxValueState:ValueState[Int]=_

            // 获取上下文
            override def open(parameters: Configuration): Unit = {
                // 定义state描述器，名称和数据类型的字节码文件
                val descriptor: ValueStateDescriptor[Int] = new ValueStateDescriptor[Int]("maxValue",classOf[Int])
                // 根绝描述器获取state
                maxValueState = getRuntimeContext.getState(descriptor)
            }

            // 遍历流数据
            override def map(value: (String, Int)) = {
                // 获取该Key对应state中保存数据，Key不需要指定，Flink自动维护映射
                val maxNum:Int=maxValueState.value()
                if(value._2>maxNum)
                    maxValueState.update(value._2)
                (value._1,maxValueState.value())
            }
        })

        maxDS.print()

        env.execute("ValueStateExp")

    }
}
