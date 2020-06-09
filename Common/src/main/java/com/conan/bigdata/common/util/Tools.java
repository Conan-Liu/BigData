package com.conan.bigdata.common.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channel;
import java.util.Random;

/**
 * 一些工具方法
 */
public class Tools {

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
}
