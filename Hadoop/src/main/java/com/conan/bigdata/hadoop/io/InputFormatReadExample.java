package com.conan.bigdata.hadoop.io;


import com.conan.bigdata.hadoop.util.HadoopConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

/**
 *
 */
public class InputFormatReadExample {

    public static void main(String[] args) throws IOException {
        Configuration conf = HadoopConf.getInstance();
        JobConf jobConf = new JobConf(conf);
        InputFormat in = new MapredParquetInputFormat();
        InputSplit[] splits = in.getSplits(jobConf, 10);
    }
}