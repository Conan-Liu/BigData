package com.conan.bigdata.hadoop.util;

import org.apache.hadoop.conf.Configuration;

/**
 * Created by Administrator on 2018/8/2.
 */
public class HadoopConf {

    private static Configuration conf = null;

    private static void init() {
        conf = new Configuration();
        // 必须指定， 否则会报 schema 不一致
        conf.set("fs.defaultFS", "hdfs://nameservice1/");
        conf.set("dfs.nameservices", "nameservice1");
        conf.set("dfs.ha.namenodes.nameservice1", "nn1,nn2");
        conf.set("dfs.namenode.rpc-address.nameservice1.nn1", "nn1.hadoop.pdbd.test.cn:8020");
        conf.set("dfs.namenode.rpc-address.nameservice1.nn2", "nn2.hadoop.pdbd.test.cn:8020");
        conf.set("dfs.client.failover.proxy.provider.ns1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("mapreduce.reduce.memory.mb", "4096");
        conf.set("mapreduce.reduce.shuffle.input.buffer.percent", "0.3");
        conf.set("mapreduce.reduce.shuffle.parallelcopies", "3");
    }

    public static Configuration getInstance() {
        if (conf == null) {
            init();
        }
        return conf;
    }

    public static void main(String[] args) {
        System.out.println(getInstance().get("dfs.namenode.rpc-address.nameservice1.nn2", "aaa"));
    }
}