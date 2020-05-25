package com.conan.bigdata.common.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channel;

/**
 * 一些工具方法
 */
public class Tools {

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
}
