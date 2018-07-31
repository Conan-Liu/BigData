package com.conan.hive.udf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class GetIpAttr extends UDF {

    //ip文件地址
    private static String ipAttrFilePath = "/user/hadoop/msc/cmcc_ip_new/current/cmcc_ip_new_20180202.txt";

    private static HashMap<Long, Long> ipRange = new HashMap<Long, Long>(); //ip段范围，key为起始ip，value为结束ip
    private static TreeMap<Long, String> ipAttrs = init();    //在加载时初始化，读取ip库数据文件并存入ipAttrs
    private Text result = new Text();

    private static TreeMap<Long, String> init() {
        TreeMap<Long, String> ipAttrs = new TreeMap<Long, String>();
        BufferedReader reader = null;
        FileSystem hdfs = null;
        String line;
        try {
            ipAttrs.put(-1L, "*\t*\t*");  //用于处理ip解析为-1的情况
            ipRange.put(-1L, Long.MAX_VALUE);
            Configuration conf = new Configuration();
            hdfs = FileSystem.get(URI.create(ipAttrFilePath), conf);
            reader = new BufferedReader(new InputStreamReader(hdfs.open(new Path(ipAttrFilePath)), "UTF-8"));

            while ((line = reader.readLine()) != null) {
                String[] ipAttrStr = line.split("\t");
                if (ipAttrStr.length >= 5) {

                    /**
                     * ip库字段说明
                     * 目前只看前5位（0-4）
                     * |   0  |  1   | 2  |     3      |    4     |    5     | ...
                     * |起始ip|结束ip|运营商|省会|所属系统|...
                     */

                    //优化：仅保存国家为'中国'的ip信息数据

                    ipAttrs.put(convertIP(ipAttrStr[0]), ipAttrStr[2] + "\t" + ipAttrStr[3] + "\t" + ipAttrStr[4]);
                    ipRange.put(convertIP(ipAttrStr[0]), convertIP(ipAttrStr[1]));

                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return ipAttrs;
    }

    public Text evaluate(Text ip) {
        if (ip == null) {
            return null;
        }

        result.set(getAttrByIp(ip.toString()));
        return result;
    }

    // 测试使用
    public Text evaluate(Text ip, String ip_attr_file) {
        if (ip == null) {
            return null;
        }

        if (ip_attr_file != null && !ipAttrFilePath.equals(ip_attr_file)) {
            ipAttrFilePath = ip_attr_file;
            ipRange.clear();
            ipAttrs = init();
        }

        result.set(getAttrByIp(ip.toString()));
        return result;
    }

    //把IP地址转换成long数值
    private static long convertIP(String ipStr) {
        try {
            long[] ipLongs = new long[4];
            String[] ips = ipStr.split("\\.");
            if (ips.length != 4) {
                return -1L;
            } else {
                for (int i = 0; i < ips.length; i++) {
                    long ipLong = Long.parseLong(ips[i]);
                    if (ipLong > 255L) {
                        return -1L;
                    }
                    ipLongs[i] = ipLong;
                }
            }
            return (ipLongs[0] << 24) + (ipLongs[1] << 16) + (ipLongs[2] << 8) + ipLongs[3];
        } catch (Exception e) {
            return -1L;
        }
    }

    private String getAttrByIp(String ipStr) {
        long ip = convertIP(ipStr);
        Map.Entry<Long, String> entry = ipAttrs.floorEntry(ip);
        long startIpRange = entry.getKey();
        String ipResult = entry.getValue();

        if (ipRange.containsKey(startIpRange)) {
            long endIpRange = ipRange.get(startIpRange);
            if (ip <= endIpRange) {
                return ipResult;
            }
        }
        return "*\t*\t*";
    }

//    public static void main(String[] args) throws Exception{

        /*
         select yanfa.migu_getipattr('222.053.252.001','/user/hadoop/msc/ip_attribute/import_day=20171101/mydata4vipday2_20171101.txt');
         select yanfa.migu_getipattr('222.053.252.001','/user/hadoop/msc/ip_attribute/import_day=20161228/mydata4vipday2_20161228.txt');
         */
//        String filePath = "C:\\Users\\shemin\\Desktop\\cmcc_ip_new_20180202.txt";
//        Map<String, String> map = new HashMap<String, String>();
//        BufferedReader br = new BufferedReader(new FileReader(filePath));
//        String tempLine;
//        int line=0;
//        while (null != (tempLine = br.readLine())) {
//            String[] fileds = tempLine.split("\t");
//            if(fileds[2].equals("中国")) {
//                map.put(fileds[0],tempLine);
//                line++;
//            }
//        }
//
//        String filePath2 = "C:\\Users\\shemin\\Desktop\\cmcc_ip_new_20180202.txt";
//        Map<String, String> map2 = new HashMap<String, String>();
//        BufferedReader br2 = new BufferedReader(new FileReader(filePath2));
//        String tempLine2;
//        int line2=0;
//        while (null != (tempLine2 = br2.readLine())) {
//            String[] fileds = tempLine2.split("\t");
//            map2.put(fileds[0], tempLine2);
//            line++;
//        }
//
//
//
//        for(Map.Entry<String, String> entry : map.entrySet()) {
//            if(!map2.containsKey(entry.getKey())) {
//                System.out.println(entry.getKey() + "\t" + entry.getValue());
//            }
//
//        }
//
//        System.out.println(line);
//        System.out.println(line2);
//        Text ip=new Text();
//        ip.set("8.8.8.8");
//
//        migu_new_getcmccipattr udf =new migu_new_getcmccipattr();
//        udf.evaluate(ip,"C:\\Users\\shemin\\Desktop\\cmcc_ip_new_20180202.txt");
//        System.out.println(udf.getAttrByIp("8.8.8.8"));
//    }

}
