package com.conan.bigdata.redis.utils;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by Administrator on 2019/5/6.
 */
public class MyJedisPool {

    //最大连接数
    public static int CONNECTION_MAX_TOTAL = 100;

    //最大空闲连接数
    public static int CONNECTION_MAX_IDLE = 50;

    //初始化连接数（最小空闲连接数）
    public static int CONNECTION_MIN_IDLE = 10;

    //等待连接的最大等待时间
    public static int CONNECTION_MAX_WAIT = 2000;

    //borrow前 是否进行alidate操作，设置为true意味着borrow的均可用
    public static boolean TEST_ON_BORROW = true;

    //return前 是否进行alidate操作
    public static boolean TEST_ON_RETURN = true;

    private static MyJedisPool myJedisPool = null;

    private static JedisPool pool = null;

    private static void initJedisPool(String ipAddress, int port, String password) {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(CONNECTION_MAX_TOTAL);
        config.setMaxIdle(CONNECTION_MAX_IDLE);
        config.setMinIdle(CONNECTION_MIN_IDLE);
        config.setMaxWaitMillis(CONNECTION_MAX_WAIT);
        config.setTestOnBorrow(TEST_ON_BORROW);
        config.setTestOnReturn(TEST_ON_RETURN);
        if ("".equals(password.trim())) {
            pool = new JedisPool(config, ipAddress, port);
        } else {
            int waitTime = 10000;
            pool = new JedisPool(config, ipAddress, port, waitTime, password);
        }
    }

    public static JedisPool getInstance(String ipAddress, int port, String password) {
        if (pool == null) {
            synchronized (JedisPool.class) {
                if (pool == null) {
                    initJedisPool(ipAddress, port, password);
                }
            }
        }
        return pool;
    }
}