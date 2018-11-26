package com.conan.bigdata.common.util;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Base64;

/**
 * Created by Administrator on 2018/11/26.
 */
public class MyBase64 {

    public static void main(String[] args) throws UnsupportedEncodingException {
        String encode= Base64.getEncoder().encodeToString("So智障".getBytes(Charset.defaultCharset()));
        System.out.println(encode);

        byte[] decode=Base64.getDecoder().decode(encode);
        System.out.println(new String(decode,"UTF-8"));
    }

}