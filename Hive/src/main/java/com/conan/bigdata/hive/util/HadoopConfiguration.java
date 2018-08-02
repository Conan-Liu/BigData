package com.conan.bigdata.hive.util;

import org.apache.hadoop.conf.Configuration;

/**
 * Created by Administrator on 2018/8/2.
 */
public class HadoopConfiguration {

    private static Configuration conf = null;

    private static void init() {
        conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://ns1/");
        conf.set("dfs.nameservices", "ns1");
        conf.set("dfs.ha.namenodes.ns1", "nn1,nn2");
        conf.set("dfs.namenode.rpc-address.ns1.nn1", "m01.dpbu.mwee.prd:8020");
        conf.set("dfs.namenode.rpc-address.ns1.nn2", "m02.dpbu.mwee.prd:8020");
        conf.set("dfs.client.failover.proxy.provider.ns1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    }

    public static Configuration getInstance() {
        if (conf == null)
            init();
        return conf;
    }

}