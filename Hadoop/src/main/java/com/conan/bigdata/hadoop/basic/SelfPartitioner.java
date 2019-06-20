package com.conan.bigdata.hadoop.basic;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by Administrator on 2019/6/19.
 */
public class SelfPartitioner<K, V> extends Partitioner<K, V> {

    /**
     * partition 的数量要和reduce的数目保持一致
     * partition 的数字是从 0 - (numPartitions - 1) 的， 所以，返回值也是要在这个区间
     *
     * @param key
     * @param value
     * @param numPartitions
     * @return
     */
    @Override
    public int getPartition(K key, V value, int numPartitions) {
        return "partition_example".hashCode() & Integer.MAX_VALUE % numPartitions;
    }
}