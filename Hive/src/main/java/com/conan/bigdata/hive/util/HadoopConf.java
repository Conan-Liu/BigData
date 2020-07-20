package com.conan.bigdata.hive.util;

import org.apache.hadoop.conf.Configuration;

import java.util.Map;

/**
 * 单例模式返回 Hadoop Configuration
 *
 * volatile + 双重检查锁 实现
 */
public class HadoopConf {

    // volatile 防止指令重排序
    private static volatile Configuration conf = null;

    private static void init() {
        conf = new Configuration();
        // 必须指定， 否则会报 schema 不一致
        conf.set("fs.defaultFS", "hdfs://localhost:9000/");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("mapreduce.reduce.memory.mb", "4096");
        conf.set("mapreduce.reduce.shuffle.input.buffer.percent", "0.3");
        conf.set("mapreduce.reduce.shuffle.parallelcopies", "3");
    }

    private static void initHA() {
        conf = new Configuration();
        // 必须指定， 否则会报 schema 不一致
        conf.set("fs.defaultFS", "hdfs://nameservice1/");
        conf.set("dfs.nameservices", "nameservice1");
        conf.set("dfs.ha.namenodes.nameservice1", "nn1,nn2");
        conf.set("dfs.namenode.rpc-address.nameservice1.nn1", "nn1.hadoop.pdbd.mwbyd.cn:8020");
        conf.set("dfs.namenode.rpc-address.nameservice1.nn2", "nn2.hadoop.pdbd.mwbyd.cn:8020");
        conf.set("dfs.client.failover.proxy.provider.nameservice1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("mapreduce.reduce.memory.mb", "4096");
        conf.set("mapreduce.reduce.shuffle.input.buffer.percent", "0.3");
        conf.set("mapreduce.reduce.shuffle.parallelcopies", "3");
    }

    public static Configuration getInstance() {
        if (conf == null) {
            synchronized (HadoopConf.class) {
                if (conf == null) {
                    init();
                }
            }
        }
        return conf;
    }

    public static Configuration getHAInstance() {
        if (conf == null) {
            synchronized (HadoopConf.class) {
                if (conf == null) {
                    initHA();
                }
            }
        }
        return conf;
    }

    public static void showConfiguration(Configuration conf) {
        for (Map.Entry<String, String> c : conf) {
            System.out.println(c.getKey() + " : " + c.getValue());
        }
    }

    public static void main(String[] args) {
//        System.out.println(getInstance().get("dfs.namenode.rpc-address.nameservice1.nn2", "aaa"));
        showConfiguration(getHAInstance());
    }
}