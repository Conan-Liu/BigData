package com.conan.bigdata.hbase.api;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.conan.bigdata.hbase.common.CONSTANT;
import com.conan.bigdata.hbase.util.HBaseUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2018/9/3.
 */
public class SimpleAPI {

    private static final String OUT_PATH = "D:\\opt\\aaa.txt";

    public static void getResult(TableName tableName, String rowKey) throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        Table table = HBaseUtils.getConnection().getTable(tableName);
        Result result = table.get(get);

        System.out.println("返回记录数: " + result.size());
        for (Cell cell : result.listCells()) {
            System.out.println(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
        }
    }

    public static void getResultScan(TableName tableName, String rowKey) {
        String startKey = rowKey;
        String stopKey = String.valueOf(Long.valueOf(rowKey) + 1);
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startKey));
        scan.setStopRow(Bytes.toBytes(stopKey));

        scan.setBatch(1000);
        scan.setCaching(1000);
        scan.setCacheBlocks(true);

        Map<String, String> map = new HashMap<>();
        List<String> jsonList = new ArrayList<>();
        File f = new File(OUT_PATH);
        ResultScanner rs = null;
        try {
            Table table = HBaseUtils.getConnection().getTable(tableName);
            rs = table.getScanner(scan);
            for (Result r : rs) {
                for (Cell cell : r.listCells()) {
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
//                    System.out.println(value);
                    String[] values = value.split("\\|");
                    if (!"点菜".equals(values[3]))
                        break;
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
//                    jsonList.add(json.toJSONString());
                    map.clear();
                    FileUtils.write(f, json.toJSONString() + "\n", Charset.defaultCharset(), true);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            rs.close();
        }


//        try {
//            Table table = HBaseUtils.getConnection().getTable(tableName);
//            rs = table.getScanner(scan);
//            for (Result r : rs) {
//                for (Cell cell : r.listCells()) {
//                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
//                    System.out.println(value);
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            rs.close();
//        }
    }


    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();
//        getResultScan(TableName.valueOf("user_tag_detail4"), "2480909613242");
        getResultScan(TableName.valueOf(CONSTANT.TABLE_NAME), new StringBuilder("162175342").reverse().toString());
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }
}