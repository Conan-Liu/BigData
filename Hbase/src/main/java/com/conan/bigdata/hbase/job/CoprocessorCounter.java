package com.conan.bigdata.hbase.job;

import com.conan.bigdata.hbase.util.HBaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;

/**
 * 协处理器统计记录数
 *
 * HBase表需要注册协处理器才可以用
 */
public class CoprocessorCounter {

    public static void main(String[] args) throws Throwable {
        long start = System.currentTimeMillis();
        Configuration conf = HBaseUtils.getHBaseConf();
        Scan scan = new Scan();
        AggregationClient aggregationClient = new AggregationClient(conf);
        long cnt = aggregationClient.rowCount(TableName.valueOf("mQueueShopState"), new LongColumnInterpreter(), scan);
        long end = System.currentTimeMillis();
        System.out.println("hbase 表总记录数 = " + cnt + "，耗时 = " + (end - start) / 1000);
    }
}
