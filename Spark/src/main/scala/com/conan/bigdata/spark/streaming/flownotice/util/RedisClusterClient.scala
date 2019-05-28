package com.conan.bigdata.spark.streaming.flownotice.util

import java.util

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{HostAndPort, Jedis, JedisCluster}

/**
  * Created by Conan on 2019/5/26.
  *
  * 如果要使用集群模式链接 Redis， 需要先启用 Redis集群， 生产一般是集群， 单机测试
  */
object RedisClusterClient {
    def getRedisClusterClient(): JedisCluster = {
        val nodes: util.HashSet[HostAndPort] = new util.HashSet[HostAndPort]()
        nodes.add(new HostAndPort("CentOS", 6379))
        val poolConfig = new GenericObjectPoolConfig()
        poolConfig.setMaxIdle(10)
        poolConfig.setMaxTotal(20)
        poolConfig.setMinIdle(4)
        poolConfig.setMaxWaitMillis(-1)
        poolConfig.setTestOnBorrow(true)
        poolConfig.setTestOnReturn(false)
        new JedisCluster(nodes, 3000, 3, poolConfig)
    }

    def getRedisClient(): Jedis = {
        val jedis = new Jedis("CentOS", 6379)
        jedis
    }
}