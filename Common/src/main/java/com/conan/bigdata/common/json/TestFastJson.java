package com.conan.bigdata.common.json;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2018/9/4.
 */
public class TestFastJson {

    public static void main(String[] args) {
        String value = "1601327661535627335279353652336|79353652|160132766|预订|1|已确认|2018-08-30 19:08:55|106661|石磨烩面馆|106664|石磨烩面馆(郑密路店)|其他美食|115|郑州|11|河南|升龙国际|{\"ordersource\":\"美味不用等电话机订单\",\"ordertime\":\"2018-09-02 19:00\",\"afternoonornight\":\"晚市\",\"position\":\"大厅\",\"dining_destination_tags\":\"\",\"accepttime\":\"0\"}";
        System.out.println(value);

        Map<String,String> map=new HashMap<>();
        String[] values = value.split("\\|");
        JSONObject json = JSON.parseObject(values[17]);
        map.put("id", values[1]);
        map.put("mw_id", values[2]);
        map.put("action", values[3]);
        map.put("state_id", values[4]);
        map.put("state_name", values[5]);
        map.put("business_time", values[6]);
        map.put("brand_id", values[7]);
        map.put("brand_name", values[8]);
        map.put("shop_id", values[9]);
        map.put("shop_name", values[10]);
        map.put("category_name", values[11]);
        map.put("city_id", values[12]);
        map.put("city_name", values[13]);
        map.put("province_id", values[14]);
        map.put("province_name", values[15]);
        map.put("bc_name", values[16]);
        json.putAll(map);
        System.out.println(json.toString());
    }

}