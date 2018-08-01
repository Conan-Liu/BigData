package com.conan.bigdata.common.javaapi;

import java.util.UUID;

/**
 * Created by Administrator on 2017/3/25.
 * 算法的核心思想是结合机器的网卡、当地时间、一个随机数来生成GUID
 */
public class TestUUID {
    public static void main(String[] args) {
        UUID uuid = UUID.randomUUID();
        System.out.println("UUID : " + uuid.toString());
    }
}
