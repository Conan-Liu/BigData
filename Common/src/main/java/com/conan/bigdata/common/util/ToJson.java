package com.conan.bigdata.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.io.*;

/**
 */
public class ToJson {

    public static void main(String[] args) throws IOException {

        BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\data\\20190909"));
        BufferedWriter bw = new BufferedWriter(new FileWriter("C:\\Users\\Administrator\\Desktop\\data\\20190909_new"));
        String line = null;
        while ((line = br.readLine()) != null) {
            JSONArray jsonArr = JSON.parseArray(line);
            for (int i = 0; i < jsonArr.size(); i++) {
                JSONObject jsonObj = jsonArr.getJSONObject(i);
                JSONObject jsonBiz = jsonObj.getJSONObject("biz");
                jsonBiz.put("shop_id", "".equals(jsonBiz.getString("shop_id")) ? 0 : Integer.parseInt(jsonBiz.getString("shop_id")));
                jsonBiz.put("brand_id", "".equals(jsonBiz.getString("brand_id")) ? 0 : Integer.parseInt(jsonBiz.getString("brand_id")));
                jsonObj.put("biz", jsonBiz.toJSONString());
                bw.write(jsonObj.toJSONString());
                bw.newLine();
            }
        }
        br.close();
        bw.close();
    }
}