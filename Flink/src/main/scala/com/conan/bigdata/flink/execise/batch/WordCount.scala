package com.conan.bigdata.flink.execise.batch

/**
  * 推荐引入包
  * import org.apache.flink.api.scala._
  * 引入这个包可以解决 could not find implicit TypeInformation 的错误
  * 用于处理Batch
  */
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.scala._
// import org.apache.flink.api.scala.ExecutionEnvironment

/**
  */
object WordCount {

    def main(args: Array[String]): Unit = {
        // 批处理程序，需要创建ExecutionEnvironment
        val env = ExecutionEnvironment.getExecutionEnvironment
        // 这里有个隐式转换，两种解决方法，第一种像注释那样直接引入一整个包
        // 或者自己实现这个隐式转换
        implicit val typeInfo1=TypeInformation.of[String](classOf[String])
        val text = env.fromElements[String]("Who's there?", "I think I hear them. Stand, ho! Who's there?", "haha")
        // map函数需要一个Tuple2的隐式
        // implicit val typeInfo2=TypeInformation.of(new TypeHint[(String,Int)](){})
        val counts = text.flatMap(_.toLowerCase.split("\\s+")).filter(_.nonEmpty)
            .map((_, 1)).groupBy(0).sum(1)

        counts.print()
    }
}