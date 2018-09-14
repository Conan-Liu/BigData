package com.conan.bigdata.hbase.api;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.conan.bigdata.hbase.common.CONSTANT;
import com.conan.bigdata.hbase.util.HBaseUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PageFilter;
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

    private static final String OUT_PATH = "D:\\opt\\eee.txt";

    public static void getResult(TableName tableName, String rowKey) throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        Table table = HBaseUtils.getConnection().getTable(tableName);
        long start = System.currentTimeMillis();
        Result result = table.get(get);

        System.out.println("返回记录数: " + result.size());
        for (Cell cell : result.listCells()) {
            System.out.println(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }

    public static void getResultScan(TableName tableName, String rowKey) {
        String startKey = rowKey;
        String stopKey = String.valueOf(Long.valueOf(rowKey) + 1);
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startKey));
        scan.setStopRow(Bytes.toBytes(stopKey));

//        PageFilter page = new PageFilter(2);
//        page.setReversed(true);
//        System.out.println(page.isReversed());
//        scan.setFilter(page);

//        scan.setReversed(true);
        scan.setBatch(1);
        scan.setCaching(1000);
        scan.setCacheBlocks(true);

        Map<String, String> map = new HashMap<>();
        List<String> jsonList = new ArrayList<>();
        File f = new File(OUT_PATH);
        ResultScanner rs = null;
//        try {
//            Table table = HBaseUtils.getConnection().getTable(tableName);
//            long start = System.currentTimeMillis();
//            rs = table.getScanner(scan);
//            for (Result r : rs) {
//                for (Cell cell : r.listCells()) {
//                    String rowKey1 = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
//                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
////                    System.out.println(value);
//                    String[] values = value.split("\001");
////                    if (!"点菜".equals(values[3]))
////                        break;
//                    JSONObject json = JSON.parseObject(values[17]);
//                    map.put("rowkey", rowKey1);
//                    map.put("id", values[0]);
//                    map.put("mw_id", values[1]);
//                    map.put("action_id", values[2]);
//                    map.put("action", values[3]);
//                    map.put("state_id", values[4]);
//                    map.put("state_name", values[5]);
//                    map.put("business_time", values[6]);
//                    map.put("brand_id", values[7]);
//                    map.put("brand_name", values[8]);
//                    map.put("shop_id", values[9]);
//                    map.put("shop_name", values[10]);
//                    map.put("category_name", values[11]);
//                    map.put("city_id", values[12]);
//                    map.put("city_name", values[13]);
//                    map.put("province_id", values[14]);
//                    map.put("province_name", values[15]);
//                    map.put("bc_name", values[16]);
//                    json.putAll(map);
//                    jsonList.add(json.toJSONString());
//                    map.clear();
//                    FileUtils.write(f, json.toJSONString() + "\n", Charset.defaultCharset(), true);
//                }
//            }
//            long end = System.currentTimeMillis();
//            System.out.println("真实数据查询时间: " + (end - start));
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            rs.close();
//        }


        try {
            Table table = HBaseUtils.getConnection().getTable(tableName);
            long start = System.currentTimeMillis();
            rs = table.getScanner(scan);
            for (Result r : rs) {
                for (Cell cell : r.listCells()) {// 13382094215292190751919927372753
//                    System.out.println(Bytes.toString(cell.getRowArray(),cell.getRowOffset(),cell.getRowLength()));
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    System.out.print(value + "\t");
                }
                break;
            }
            long end = System.currentTimeMillis();
            System.out.println(end - start);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            rs.close();
        }
    }


    private static String getAction(int id) {
        String a = "";
        switch (id) {
            case 1:
                a = "排队";
                break;
            case 2:
                a = "预定";
                break;
            case 3:
                a = "点菜";
                break;
            case 4:
                a = "支付";
                break;
            case 5:
                a = "评价";
                break;
            case 6:
                a = "储值";
                break;
            case 7:
                a = "积分";
                break;
            case 8:
                a = "优惠券";
                break;
            default:
                a = "全部";
                break;

        }
        return a;
    }

    public static void getDataByPageSize(TableName tableName, String rowKey, int actionId, int page, int pageSize) {
        String startKey = rowKey;
        String stopKey = rowKey + "B";
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startKey));
        scan.setStopRow(Bytes.toBytes(stopKey));

        scan.setBatch(1);
        scan.setCaching(1000);
        scan.setCacheBlocks(true);

        scan.setReversed(true);
        String action = getAction(actionId);

        File f = new File(OUT_PATH);
        Map<String, String> map = new HashMap<>();
        List<String> jsonList = new ArrayList<>();
        int count = 0;
        int startRow = (page - 1) * pageSize;
        ResultScanner rs = null;
        try {
            Table table = HBaseUtils.getConnection().getTable(tableName);
            long start = System.currentTimeMillis();
            rs = table.getScanner(scan);
            for (Result r : rs) {
                count++;
                if (count <= startRow)
                    continue;
                else if (count > (startRow + pageSize)) {
                    continue;
                }
                for (Cell cell : r.listCells()) {
                    String rowKey1 = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    String[] values = value.split("\001");
//                    System.out.println(values.length);
                    if (actionId != 9999 && !action.equals(values[3]))
                        break;

                    JSONObject json = JSON.parseObject(values[17]);
                    map.put("rowkey", rowKey1);
                    map.put("id", values[0]);
                    map.put("mw_id", values[1]);
                    map.put("action_id", values[2]);
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
                    jsonList.add(json.toJSONString());
                    map.clear();
//                    FileUtils.write(f, json.toJSONString() + "\n", Charset.defaultCharset(), true);
                }
            }
            long end = System.currentTimeMillis();
            System.out.println("真实数据查询时间: " + (end - start));
            for (String json : jsonList)
                FileUtils.write(f, json + "\n", Charset.defaultCharset(), true);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            rs.close();
        }
    }

    public static void getRegionInfo(TableName tableName) {
        try {
            Admin admin = HBaseUtils.getHbaseAdmin();
            List<HRegionInfo> regions = admin.getTableRegions(tableName);
            for (HRegionInfo region : regions) {
                System.out.println(Bytes.toString(region.getEndKey()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String lpadMwid(String str) {
        String fullStr;
        switch (str.length()) {
            case 1:
                fullStr = "00000000" + str;
                break;
            case 2:
                fullStr = "0000000" + str;
                break;
            case 3:
                fullStr = "000000" + str;
                break;
            case 4:
                fullStr = "00000" + str;
                break;
            case 5:
                fullStr = "0000" + str;
                break;
            case 6:
                fullStr = "000" + str;
                break;
            case 7:
                fullStr = "00" + str;
                break;
            case 8:
                fullStr = "0" + str;
                break;
            case 9:
                fullStr = str;
                break;
            default:
                fullStr = "999999999";
                break;
        }
        return fullStr;
    }

    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();
        getResultScan(TableName.valueOf("user_tag_detail"), "31305617");
//        getResultScan(TableName.valueOf(CONSTANT.TABLE_NAME), new StringBuilder("9453162").reverse().toString());
        // 162175342
//        getDataByPageSize(TableName.valueOf(CONSTANT.TABLE_NAME), new StringBuilder(lpadMwid("162175342")).reverse().toString(), 9999, 1, 10);
//        getRegionInfo(TableName.valueOf(CONSTANT.TABLE_NAME));
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }
}