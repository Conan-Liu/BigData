package com.conan.bigdata.hive.util;

import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * {@link org.apache.hadoop.hive.ql.metadata.Hive} 可以直接操作元数据
 */
public class HiveMetaData {


    public static void main(String[] args) throws HiveException {
        Hive hive=Hive.get();

        hive.getPartitionNames("","",Short.MAX_VALUE);
    }

}