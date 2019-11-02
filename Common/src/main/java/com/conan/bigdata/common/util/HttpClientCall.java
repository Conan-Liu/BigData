package com.conan.bigdata.common.util;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.methods.StringRequestEntity;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;

/**
 */
public class HttpClientCall {

    private static final String URL_PATH = "http://api.basic.biz.mwbyd.cn/services/api/city.locationCity";

    // apache commons-http 处理http请求
    public static void doPost1() {
        PostMethod method = new PostMethod(URL_PATH);
        HttpClient httpClient = new HttpClient();
        try {
            RequestEntity entity = new StringRequestEntity("{\"longitude\":\"113.361862\",\"latitude\":\"22.941324\"}", "application/json", "UTF-8");
            method.setRequestEntity(entity);
            httpClient.executeMethod(method);
            InputStream in = method.getResponseBodyAsStream();
            StringBuffer sb = new StringBuffer();
            InputStreamReader isr = new InputStreamReader(in, "UTF-8");
            char[] b = new char[4096];
            int len = 0;
            while ((len = isr.read(b)) != -1) {
                sb.append(new String(b, 0, len));
            }
            System.out.println(sb.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // jdk 原生的http 请求处理
    public static void doPost2() {
        try {
            URL restURL = new URL(URL_PATH);
            HttpURLConnection httpConn = (HttpURLConnection) restURL.openConnection();
            httpConn.setRequestMethod("POST");
            httpConn.setRequestProperty("Content-Type", "application/json");
            httpConn.setDoOutput(true);
            httpConn.setAllowUserInteraction(false);
            OutputStream out = httpConn.getOutputStream();
            // 可以使用 BufferedReader 利用缓冲流来处理输入的数据
            out.write("{\"longitude\":\"113.361862\",\"latitude\":\"22.941324\"}".getBytes());
            out.flush();
            // 这里要注意了， 先发送请求， 才能获取服务器的返回， 否则报400 Bad Request的错误
            InputStream in = httpConn.getInputStream();
            // 获取返回码
            int resultCode = httpConn.getResponseCode();
            if (HttpURLConnection.HTTP_OK == resultCode) {
                byte[] b = new byte[1024];
                int len;
                StringBuilder sb=new StringBuilder();
                while ((len = in.read(b)) != -1) {
                    sb.append(new String(b, 0, len, "UTF-8"));
                }
                System.out.println(sb);
            }
            out.close();
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println("commons-https 请求");
        doPost1();
        System.out.println("jdk 原生api请求");
        doPost2();
    }

}