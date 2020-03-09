package com.conan.bigdata.hive.fileformat;

import com.conan.bigdata.hadoop.util.HadoopConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class ParquetWriter {
    public static void main(String[] args) throws IOException {
        ParquetHiveSerDe parquetHiveSerDe=new ParquetHiveSerDe();
        Configuration conf = HadoopConf.getInstance();
        FileSystem fs = FileSystem.get(conf);
        FileOutputFormat outputFormat = new MapredParquetOutputFormat();
        RecordWriter writer = outputFormat.getRecordWriter(fs, new JobConf(conf), "hdfs://ns1/user/hadoop/aaa/test", Reporter.NULL);

    }
}