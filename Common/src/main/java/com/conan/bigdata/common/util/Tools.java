package com.conan.bigdata.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channel;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

/**
 * 一些工具方法
 */
public class Tools {

    private static final Logger log = LoggerFactory.getLogger(Tools.class);

    private static final Random random = new Random();

    public static void closeInputStream(InputStream in) {
        try {
            if (in != null) {
                in.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void closeChannel(Channel channel) {
        try {
            if (channel != null) {
                if (channel.isOpen()) {
                    channel.close();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void randomSleep(int sleepTime, boolean isRandom) {
        if (sleepTime <= 0) {
            throw new IllegalArgumentException("sleepTime must be positive number");
        }
        try {
            if (isRandom) {
                int randTime = random.nextInt(sleepTime);
                Thread.sleep(randTime);
            } else {
                Thread.sleep(sleepTime);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static String byteToHexString(byte[] binary) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < binary.length; i++) {
            // 二进制byte转int，一定需要 byte & 0xFF，这里面涉及到补码的问题，负数的补码前缀全是1，这是为了去掉前缀
            String hex = Integer.toHexString(binary[i] & 0xFF);
            if (hex.length() < 2) {
                sb.append("0");
            }
            sb.append(hex);
        }
        return sb.toString();
    }

    public static Properties getConfig() {
        Properties properties = new Properties();
        try {
            // class.getClassLoader().getResourceAsStream("application.properties")  相对路径
            // class.getResourceAsStream("/application.properties")  绝对路径，要加 /，这个绝对路径是classpath根目录的路径
            InputStream in = Tools.class.getClassLoader().getResourceAsStream("application.properties");
            properties.load(in);
            in.close();
            return properties;
        } catch (IOException e) {
            log.error("config.properties 加载失败");
            return properties;
        }
    }

    public static void main(String[] args) {
        Properties properties = getConfig();
        Enumeration<?> enumeration = properties.propertyNames();
        String key;
        while (enumeration.hasMoreElements()) {
            key = (String) enumeration.nextElement();
            System.out.println(key + "=" + properties.get(key));
        }
        System.out.println("end ===");
    }
}
