package com.conan.bigdata.hbase.mwee;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Created by Administrator on 2018/11/5.
 */
public class TestHbase {

    private static Configuration hbaseConf = null;
    private static Connection connection = null;
    private static Admin hbaseAdmin = null;

    public static Configuration getHBaseConf() {
        if (hbaseConf == null) {
            hbaseConf = HBaseConfiguration.create();
            // 必须指定， 否则会报 schema 不一致
            hbaseConf.set("fs.defaultFS", "hdfs://nameservice1/");
            hbaseConf.set("dfs.nameservices", "nameservice1");
            hbaseConf.set("dfs.ha.namenodes.nameservice1", "nn1,nn2");
            hbaseConf.set("dfs.namenode.rpc-address.nameservice1.nn1", "nn1.hadoop.pdbd.mwbyd.cn:8020");
            hbaseConf.set("dfs.namenode.rpc-address.nameservice1.nn2", "nn2.hadoop.pdbd.mwbyd.cn:8020");
            hbaseConf.set("dfs.client.failover.proxy.provider.ns1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
            hbaseConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            hbaseConf.set("mapreduce.reduce.memory.mb", "4096");
            hbaseConf.set("mapreduce.reduce.shuffle.input.buffer.percent", "0.3");
            hbaseConf.set("mapreduce.reduce.shuffle.parallelcopies", "3");
            hbaseConf.set("mapreduce.map.maxattempts", "2");
            hbaseConf.set("hbase.zookeeper.quorum", "10.1.39.98,10.1.39.99,10.1.39.100");
            hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
        }
        return hbaseConf;
    }

    public static Connection getConnection() throws IOException {
        if (connection == null) {
            connection = ConnectionFactory.createConnection(getHBaseConf());
        }
        return connection;
    }

    public static Admin getHbaseAdmin() throws IOException {
        if (hbaseAdmin == null) {
            hbaseAdmin = getConnection().getAdmin();
        }
        return hbaseAdmin;
    }

    public static void show(ResultScanner rs) {
        try {
            for (Result r : rs) {
                for (Cell cell : r.rawCells()) {
                    System.out.print(new String(CellUtil.cloneQualifier(cell)) + ":" + new String(CellUtil.cloneValue(cell), "UTF-8") + "\t");
                }
                System.out.println();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void getScanner(String searchKey) throws Exception {
        String startKey = reverseWithLpad(searchKey);
        String stopKey = reverseWithLpad(searchKey) + "B";

        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startKey));
        scan.setStopRow(Bytes.toBytes(stopKey));
        scan.setBatch(1);
        scan.setCaching(1000);
        scan.setCacheBlocks(true);

        ResultScanner rs = getConnection().getTable(TableName.valueOf("user_tag_detail")).getScanner(scan);
        show(rs);
    }

    public static String reverseWithLpad(String searchKey) {
        int len = searchKey.length();
        String key = "";
        switch (len) {
            case 1:
                key = "000000000" + searchKey;
                break;
            case 2:
                key = "00000000" + searchKey;
                break;
            case 3:
                key = "0000000" + searchKey;
                break;
            case 4:
                key = "000000" + searchKey;
                break;
            case 5:
                key = "00000" + searchKey;
                break;
            case 6:
                key = "0000" + searchKey;
                break;
            case 7:
                key = "000" + searchKey;
                break;
            case 8:
                key = "00" + searchKey;
                break;
            case 9:
                key = "0" + searchKey;
                break;
            case 10:
                key = searchKey;
                break;
            default:
                key = "";
        }
        return new StringBuilder(key).reverse().toString();
    }


    public static void main(String[] args) throws Exception{
        getScanner("219399682");
    }
}