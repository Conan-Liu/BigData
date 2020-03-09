package com.conan.bigdata.common.javaapi;

import java.util.UUID;

/**
 * 算法的核心思想是结合机器的网卡、当地时间、一个随机数来生成GUID
 * UUID 唯一标识符， 目前全球通用的是微软（GUID）的 8-4-4-4-12 的标准, 占36个字符
 */
public class TestUUID {
    public static void main(String[] args) {
        UUID uuid = UUID.randomUUID();
        System.out.println("UUID : " + uuid.toString());
    }
}
