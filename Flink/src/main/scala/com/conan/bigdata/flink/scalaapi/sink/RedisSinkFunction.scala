package com.conan.bigdata.flink.scalaapi.sink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  * RedisSink演示
  * 该功能相对简单，如果需要更多的功能，可以继承抽象类[[RichSinkFunction]]，自定义实现
  */
object RedisSinkFunction {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val textStream: DataStream[String] = env.socketTextStream("localhost", 9999)

        val sum: DataStream[(String, Int)] = textStream.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)

        val redisConfig: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build()

        sum.addSink(new RedisSink[(String,Int)](redisConfig, new MyRedisMapper))

        env.execute("RedisSinkFunction")
    }
}

class MyRedisMapper extends RedisMapper[(String, Int)] {
    // 获取命令描述器,使用hash结构
    override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET, "keyname")
    }

    override def getKeyFromData(t: (String, Int)): String = {
        t._1
    }

    override def getValueFromData(t: (String, Int)): String = {
        t._2.toString
    }
}

