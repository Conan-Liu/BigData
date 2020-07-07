package com.conan.bigdata.common.algorithm;

import com.conan.bigdata.common.util.Tools;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * url短链算法
 * 请求短链，从短链服务获取短链对应的完整url，然后跳转url，多一次请求
 * 1. 自己实现
 * 2. 调用第三方接口
 */
public class ShortUrl {

    char[] chars = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g',
            'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T',
            'U', 'V', 'W', 'X', 'Y', 'Z'};

    public static void main(String[] args) throws IOException {
        callInterface();
    }


    /**
     * 调用第三方接口
     */
    public static void callInterface() throws IOException {
        // 这是腾讯的，需要授权key，可以申请
        String apiUrl = "http://lnurl.cn/urlcn/api?key=授权KEY&url=";
        String longUrl = "http://www.baidu.com";
        URL url = new URL(apiUrl + longUrl);
        InputStream in = url.openStream();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            byte[] buf = new byte[4096];
            int read = 0;
            while ((read = in.read(buf)) > 0) {
                out.write(buf, 0, read);
            }
        } finally {
            Tools.closeInputStream(in);
        }
        byte[] result = out.toByteArray();
        System.out.println(new String(result, "utf-8"));
    }
}
