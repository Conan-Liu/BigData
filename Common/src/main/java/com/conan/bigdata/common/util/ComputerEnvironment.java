package com.conan.bigdata.common.util;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 */
public class ComputerEnvironment {

    public static void main(String[] args) {
        System.out.println("************** 电脑环境信息 *********************************************\n");
        Map<String, String> environments = System.getenv();
        for (Map.Entry<String, String> env : environments.entrySet()) {
            System.out.println(env.getKey() + " = " + env.getValue());
        }

        System.out.println("\n************** JVM环境信息 *********************************************");
        Properties properties = System.getProperties();
        Set<Map.Entry<Object, Object>> entries = properties.entrySet();
        for (Map.Entry<Object, Object> entry : entries) {
            System.out.println(String.valueOf(entry.getKey()) + " = " + String.valueOf(entry.getValue()));
        }
    }
}