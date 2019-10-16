package com.conan.bigdata.hadoop.io;


import com.conan.bigdata.hadoop.util.HadoopConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.parquet.hadoop.ParquetInputFormat;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class HDFSReadExample {

    // 老版 InputFormat 读取 Parquet MapredParquetInputFormat
    public static void read1(Configuration conf) throws IOException {
        JobConf jobConf = new JobConf(conf);
        InputFormat<Void, ArrayWritable> in = new MapredParquetInputFormat();
        FileInputFormat.addInputPath(jobConf, new Path("/user/hive/warehouse/ods.db/resta_shoptable/*"));
        InputSplit[] splits = in.getSplits(jobConf, 1);
        for (InputSplit split : splits) {
            RecordReader reader = in.getRecordReader(split, jobConf, Reporter.NULL);
            Object K = reader.createKey();
            Object V = reader.createValue();
            int n = 0;
            while (reader.next(K, V)) {
                n++;
                Writable[] vs = ((ArrayWritable) V).get();
                String shopId = String.valueOf(vs[0]);
                String shopName = String.valueOf(vs[9]);
                System.out.println(shopId + "\t" + shopName);
                if (n >= 30) {
                    break;
                }
            }
        }
    }

    // 新版 InputFormat 读取 Parquet ParquetInputFormat
    public static void read2(Configuration conf) throws IOException, InterruptedException {
        Job job = Job.getInstance(conf);
        org.apache.hadoop.mapreduce.InputFormat in = new ParquetInputFormat();
        ParquetInputFormat.addInputPath(job, new Path("/user/hive/warehouse/ods.db/resta_shoptable/*"));
        List<org.apache.hadoop.mapreduce.InputSplit> splits = in.getSplits(job);
        for (org.apache.hadoop.mapreduce.InputSplit split : splits) {
            org.apache.hadoop.mapreduce.RecordReader reader = in.createRecordReader(split, null);
            Object K = reader.getCurrentKey();
            Object V = reader.getCurrentValue();
            int n = 0;
            while (reader.nextKeyValue()) {
                n++;
                Writable[] vs = ((ArrayWritable) V).get();
                String shopId = String.valueOf(vs[0]);
                String shopName = String.valueOf(vs[9]);
                System.out.println(shopId + "\t" + shopName);
                if (n >= 30) {
                    break;
                }
            }
        }
    }

    /**
     * FSDataInputStream 读取数据
     * 按照流读取数据，可以作为读取文件，
     * 但是流是字节的， 如果是文本文件， 可以直接显示内容， 如果是二进制文件（如parquet）， 不可直接显示内容
     */
    public static void read3(Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream in = fs.open(new Path("/tmp/aaa"));
        IOUtils.copyBytes(in, System.out, 4096, false);
        // 重定向数据流的指针到开始， seek 很耗资源， 最好不用
        in.seek(0);
        IOUtils.copyBytes(in, System.out, 4096, false);
        IOUtils.closeStream(in);
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = HadoopConf.getHAInstance();
        read1(conf);

//        read2(conf);
    }
}