package com.conan.bigdata.redis;

import redis.clients.jedis.Jedis;

import java.util.Iterator;
import java.util.Set;

public class App {

    public static void main(String[] args) {
        Jedis jedis = new Jedis("CentOS", 6379);
        System.out.println("是否链接成功: " + jedis.isConnected());  // 这个居然是false
        System.out.println("服务正在运行: " + jedis.ping());

        System.out.println();

        jedis.set("mykey_1", "str_1");
        jedis.set("mykey_2", "str_2");
        System.out.println("mykey_1: " + jedis.get("mykey_1"));
        System.out.println("总记录数: " + jedis.dbSize());

        System.out.println();

        jedis.lpush("site-list", "runoob");
        jedis.lpush("site-list", "google");
        jedis.lpush("site-list", "taobao");
        System.out.println("List实例: site-list = " + jedis.lpop("site-list"));
        System.out.println("List实例: site-list = " + jedis.rpop("site-list"));

        System.out.println();

        Set<String> keys = jedis.keys("*");
        Iterator<String> it = keys.iterator();
        System.out.print("redis包含的key: ");
        while (it.hasNext()) {
            String key = it.next();
            System.out.print("\t" + key);
        }
    }
}