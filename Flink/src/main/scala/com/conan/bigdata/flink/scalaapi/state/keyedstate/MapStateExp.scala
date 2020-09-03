package com.conan.bigdata.flink.scalaapi.state.keyedstate

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
  * [[MapState]]保存中间结果计算分组的和
  */
object MapStateExp {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val collection: DataStream[(String, Int)] = env.fromCollection(List(
            ("java", 1),
            ("python", 3),
            ("java", 2),
            ("scala", 2),
            ("python", 1),
            ("java", 1),
            ("scala", 2)
        ))

        val by: KeyedStream[(String, Int), Tuple] = collection.keyBy(0)

        // 转换MapState，同样需要RichFunction
        val map1: DataStream[(String, Int)] = by.map(new RichMapFunction[(String, Int), (String, Int)] {

            var state: MapState[String, Int] = _

            // 获取MapState
            override def open(parameters: Configuration): Unit = {
                // 定义一个MapState描述器
                val descriptor: MapStateDescriptor[String, Int] = new MapStateDescriptor[String, Int](
                    "sumMap",
                    // 使用TypeInformation来包装KV数据类型
                    TypeInformation.of(classOf[String]),
                    TypeInformation.of(classOf[Int])
                )

                state = getRuntimeContext.getMapState(descriptor)
            }

            override def map(value: (String, Int)): (String, Int) = {
                // 处理新数据
                val key = value._1
                // 根据key获取MapState的历史数据，并更新数据
                val i: Int = state.get(key)
                state.put(key, i + value._2)

                (key, state.get(key))
            }
        })

        map1.print()

        env.execute("MapStateExp")

    }
}
