package com.conan.bigdata.hbase.util;

import com.conan.bigdata.hadoop.util.HadoopConf;
import com.conan.bigdata.hbase.mr.CONSTANT;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by Administrator on 2018/8/28.
 */
public class HBaseUtils {

    private static Configuration hbaseConf = null;
    private static Admin hbaseAdmin = null;

    public static byte[][] getSplitKey(String key, String separator) {
        String[] keys = key.split(separator);
        int len = keys.length;
        byte[][] splitKey = new byte[len][];
        for (int i = 0; i < len; i++) {
            splitKey[i] = Bytes.toBytes(keys[i]);
        }
        return splitKey;
    }

    public static Configuration getHBaseConf() {
        if (hbaseConf == null) {
            hbaseConf = HBaseConfiguration.create();
            hbaseConf = new Configuration();
            // 必须指定， 否则会报 schema 不一致
            hbaseConf.set("fs.defaultFS", "hdfs://nameservice1/");
            hbaseConf.set("dfs.nameservices", "nameservice1");
            hbaseConf.set("dfs.ha.namenodes.nameservice1", "nn1,nn2");
            hbaseConf.set("dfs.namenode.rpc-address.nameservice1.nn1", "nn1.hadoop.pdbd.prod:8020");
            hbaseConf.set("dfs.namenode.rpc-address.nameservice1.nn2", "nn2.hadoop.pdbd.prod:8020");
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

    public static Admin getHbaseAdmin() throws IOException {
        if (hbaseAdmin == null) {
            Connection conn = ConnectionFactory.createConnection(HBaseUtils.getHBaseConf());
            hbaseAdmin = conn.getAdmin();
        }
        return hbaseAdmin;
    }

    public static boolean deleteDir() throws IOException {
        FileSystem fs=FileSystem.get(getHBaseConf());
        Path out=new Path(CONSTANT.OUTPUT_PATH);
        if(fs.exists(out)){
            fs.delete(out,true);
            return true;
        }
        return false;
    }
}