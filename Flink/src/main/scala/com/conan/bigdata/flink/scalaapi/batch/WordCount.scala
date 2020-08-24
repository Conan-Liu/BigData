package com.conan.bigdata.flink.scalaapi.batch

/**
  * 推荐引入包
  * import org.apache.flink.api.scala._
  * 引入这个包可以解决 could not find implicit TypeInformation 的错误
  * 用于处理Batch
  *
  * 八股文编程
  * 1. 创建一个execution environment
  * 2. 创建加载源数据Source
  * 3. 对数据进行Transformation（这一步可以没有）
  * 4. 指定计算结果的输出Sink
  * 5. 触发程序执行
  */

import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.scala._
// import org.apache.flink.api.scala.ExecutionEnvironment

/**
  */
object WordCount {

    def main(args: Array[String]): Unit = {
        // 1. 创建一个execution environment
        val env = ExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        // 这里有个隐式转换，两种解决方法，第一种像注释那样直接引入一整个包
        // 或者自己实现这个隐式转换
        implicit val typeInfo1 = TypeInformation.of[String](classOf[String])
        // 2. 创建加载源数据Source
        val text = env.fromElements[String]("Who's there?", "I think I hear them. Stand, ho! Who's there?", "haha")

        // 3. 对数据进行Transformation
        val counts: DataSet[(String, Int)] = text.flatMap(_.toLowerCase.split("\\s+")).filter(_.nonEmpty).map((_, 1))
        val sum = counts.groupBy(0).sum(1)

        // 4. 指定计算结果的输出Sink
        sum.print()
        // sum.write()

        // 5. 触发程序执行
        // 注意：在批处理时，如果Sink操作时count() collect() print()操作，那么这个execute()不需要使用，否则报错，因为这几个方法中已经包含了execute()方法，和八股文编程并不冲突
        // env.execute("WordCount")
    }
}