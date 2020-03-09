package com.conan.bigdata.redis;

import com.conan.bigdata.redis.utils.JedisPoolUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class MyJedisPool {
    public static void main(String[] args) throws InterruptedException {
        JedisPool pool = JedisPoolUtil.getJedisPoolInstance("CentOS", 6379, "");
        for (int i = 0; i < 10; i++) {
            Jedis jedis = pool.getResource();
//            System.out.println("name = " + jedis.get("name"));
//            jedis.set("name", "V");
//            System.out.println("name = " + jedis.get("name"));
            System.out.println("pool地址: " + pool + "\t\tjedis地址: " + jedis);
//            pool.close();
        }
        Thread.sleep(1000000);
    }
}