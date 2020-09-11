package com.conan.bigdata.flink.scalaapi.streaming

import java.util.concurrent.TimeUnit

import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import scala.concurrent.{ExecutionContext, Future}

/**
  * 演示异步IO
  * 读取集合中的城市id使用异步IO去redis中读取城市名称
  * 异步IO可以配合redis，hbase操作
  * 查询外部系统，可以考虑使用异步IO
  */
object AsyncIOExp {

    def main(args: Array[String]): Unit = {
        val env=StreamExecutionEnvironment.getExecutionEnvironment

        val cityDS=env.fromCollection(List[String]("1","2","3","4","5","6","7","8","9"))

        // 使用工具类AsyncDataStream来实现异步IO，包括两个方法orderedWait和unorderedWait
        val wait = AsyncDataStream.unorderedWait(
            cityDS,
            new MyAsyncFunction,
            1000,
            TimeUnit.MILLISECONDS
        )
        wait.print()
        env.execute("")
    }
}

// 定义AsyncIO对象
class MyAsyncFunction extends RichAsyncFunction[String,String]{

    var jedis:Jedis=_

    override def open(parameters: Configuration): Unit = {
        val config: JedisPoolConfig = new JedisPoolConfig()
        config.setLifo(true)
        config.setMaxTotal(10)
        config.setMaxIdle(10)
        // 设置连接时的最大等待毫秒数，如果超时跑异常，小于零则表示阻塞不确定时间，默认-1
        config.setMaxWaitMillis(-1)
        // 逐出连接的最小空闲时间，默认30分钟
        config.setMinEvictableIdleTimeMillis(1800000)
        // 最小空闲连接数，默认0
        config.setMinIdle(0)
        // 每次逐出检查时，逐出的最大数目，如果为负数
        config.setNumTestsPerEvictionRun(3)
        // 对象空闲多久后逐出，当空闲时间
        config.setSoftMinEvictableIdleTimeMillis(1800000)
        // 在获取连接的时候检查有效性，默认false
        config.setTestOnBorrow(false)
        // 在空闲时检查有效性，默认false
        config.setTestWhileIdle(false)

        val pool: JedisPool = new JedisPool(config,"localhost",6379)
        jedis=pool.getResource
    }

    override def close(): Unit = {
        jedis.close()
    }

    // 处理异步请求超时的方法，默认超时报异常
    override def timeout(input: String, resultFuture: ResultFuture[String]): Unit = {
        println("input >>"+input+",,,请求超时...")
    }

    implicit lazy val executor=ExecutionContext.fromExecutor(Executors.directExecutor())

    // 异步请求方法，输入数据，异步请求结果对象
    override def asyncInvoke(input: String, resultFuture: ResultFuture[String]): Unit = {
        // 执行异步请求，请求结果封装在resultFuture中
        Future {
            val hget: String = jedis.hget("key", input)
            // 异步回调返回
            resultFuture.complete(Array(hget))
        }
    }
}