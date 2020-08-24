package com.conan.bigdata.flink.scalaapi.source

import org.apache.flink.streaming.api.scala._

/**
  */
object CollectionStreamExp {

    def main(args: Array[String]): Unit = {
        val env=StreamExecutionEnvironment.getExecutionEnvironment
        val stream: DataStream[String] = env.fromElements[String]("spark","flink")
        stream.print()

        env.execute("CollectionStreamExp")
    }
}
