package com.conan.bigdata.spark.utils

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
  * 广播这个连接池对象即可
  */
object RedisPool extends Serializable {

    // 不参与序列化
    @transient private var pool: JedisPool = _

    lazy val redisConfig = {
        val config = new JedisPoolConfig
        config.setMaxTotal(100)
        config.setMaxIdle(5)
        config.setMaxWaitMillis(1000)
        config.setTestOnBorrow(true)
        config
    }

    def getPool: JedisPool = {
        if (pool == null) {
            pool = new JedisPool(redisConfig, "host", 6347, 1000, "pass")

            // 当jvm关闭的时候，会执行系统中已经设置的所有通过方法addShutdownHook添加的钩子
            // 当系统执行完这些钩子后, jvm才会关闭。所以可通过这些钩子在jvm关闭的时候进行内存清理、资源回收等工作
            sys.addShutdownHook{
                pool.destroy()
            }
        }
        pool
    }

}