package com.conan.bigdata.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.methods.StringRequestEntity;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;

/**
 */
public class HttpClientCall {

    // private static final String URL_PATH = "http://api.basic.biz.mwbyd.cn/services/api/city.locationCity";
    private static final String URL_PATH = "http://alarm-notify.mwbyd.cn/services/notify/pushAll";
    // 高德 gps -> 城市地里位置
    private static final String GD_PATH = "https://restapi.amap.com/v3/geocode/regeo?key=bcb8922ebe8b072b4a54c7c33e0b8c2f&location=";

    // apache commons-http 处理http请求
    public static String doPost1(String content) {
        HttpClient httpClient = new HttpClient();
        PostMethod method = new PostMethod(URL_PATH);
        try {
            RequestEntity entity = new StringRequestEntity(content, "application/json", "UTF-8");
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
            return sb.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    // jdk 原生的http 请求处理
    public static void doPost2(String content) {
        try {
            URL restURL = new URL(URL_PATH);
            HttpURLConnection httpConn = (HttpURLConnection) restURL.openConnection();
            httpConn.setRequestMethod("POST");
            httpConn.setRequestProperty("Content-Type", "application/json");
            httpConn.setDoOutput(true);
            httpConn.setAllowUserInteraction(false);
            OutputStream out = httpConn.getOutputStream();
            // 可以使用 BufferedReader 利用缓冲流来处理输入的数据
            out.write(content.getBytes());
            out.flush();
            // 这里要注意了， 先发送请求， 才能获取服务器的返回， 否则报400 Bad Request的错误
            InputStream in = httpConn.getInputStream();
            // 获取返回码
            int resultCode = httpConn.getResponseCode();
            if (HttpURLConnection.HTTP_OK == resultCode) {
                byte[] b = new byte[1024];
                int len;
                StringBuilder sb = new StringBuilder();
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

    // apache commons-http 处理http请求
    public static String doGet(String url) {
        HttpClient httpClient = new HttpClient();
        GetMethod method = new GetMethod(url);
        try {
            method.setRequestHeader("Content-Type", "application/json");
            httpClient.executeMethod(method);
            String result = method.getResponseBodyAsString();
            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    public static void doMwRequest() {

    }

    public static void doGaoDeRequet() throws IOException, InterruptedException {
        System.out.println("commons-https 请求");
        BufferedReader br = new BufferedReader(new FileReader("c:\\111"));
        BufferedWriter bw = new BufferedWriter(new FileWriter("c:\\222"));
        String line;
        StringBuilder sb = new StringBuilder();
        int n = 0;
        while ((line = br.readLine()) != null) {
            String[] fields = line.split("\\|");
            System.out.println(Arrays.toString(fields));
            String returnJson = doGet(GD_PATH + fields[2] + "," + fields[3]);
            JSONObject jsonOut = JSON.parseObject(returnJson);
            String city = jsonOut == null ? "" : jsonOut.getJSONObject("regeocode").getJSONObject("addressComponent").getString("city");
            String province = jsonOut == null ? "" : jsonOut.getJSONObject("regeocode").getJSONObject("addressComponent").getString("province");
            sb.append(fields[0]).append("|").append(fields[1]).append("|").append(province).append("|").append(city).append("\n");
            bw.write(sb.toString());
            n++;
            Thread.sleep(100);
            sb.delete(0, sb.length());
        }
        System.out.println("总数: " + n);
        bw.flush();
        bw.close();
        br.close();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        // doGaoDeRequet();
        String content1="{\"longitude\":\"113.361862\",\"latitude\":\"22.941324\"}";
        String content2="{\"type\":\"2\",\"receiverMobiles\":\"13852293070\",\"subject\":\"测试 subject\",\"content\":\"logContent\"}";
        doPost2(content2);
    }

}