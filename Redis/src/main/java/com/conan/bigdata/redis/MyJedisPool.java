package com.conan.bigdata.redis;

import com.conan.bigdata.redis.utils.JedisPoolUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Created by Administrator on 2019/5/7.
 */
public class MyJedisPool {
    public static void main(String[] args) {
        JedisPool pool = JedisPoolUtil.getJedisPoolInstance("CentOS", 6379, "");
        Jedis jedis = pool.getResource();
        System.out.println("name = " + jedis.get("name"));
        jedis.set("name", "V");
        System.out.println("name = " + jedis.get("name"));
        pool.close();
    }
}