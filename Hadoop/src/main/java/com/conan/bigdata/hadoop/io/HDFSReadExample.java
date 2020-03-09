package com.conan.bigdata.hadoop.io;


import com.conan.bigdata.hadoop.util.HadoopConf;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.hadoop.ParquetInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class HDFSReadExample {

    // 老版 InputFormat 读取 Parquet MapredParquetInputFormat
    public static void read1(Configuration conf) throws IOException {
        JobConf jobConf = new JobConf(conf);
        InputFormat<Void, ArrayWritable> in = new MapredParquetInputFormat();
        FileInputFormat.addInputPath(jobConf, new Path("/user/hive/warehouse/dmd.db/dm_dashboard_bi_report_all_y_output/000000_0"));
        InputSplit[] splits = in.getSplits(jobConf, 1);
        for (InputSplit split : splits) {
            RecordReader reader = in.getRecordReader(split, jobConf, Reporter.NULL);
            Object K = reader.createKey();
            Object V = reader.createValue();
            int n = 0;
            while (reader.next(K, V)) {
                n++;
                if (n <= 2) {
                    Writable[] vs = ((ArrayWritable) V).get();
                    StringBuilder sb = new StringBuilder();
                    for (Writable v : vs) {
                        sb.append(String.valueOf(v)).append("|");
                    }
                    System.out.println(sb.toString());
                }
            }
            System.out.println("======================= " + n);
        }
    }

    // 老版 InputFormat 读取 Parquet MapredParquetInputFormat
    public static void read11(Configuration conf) throws IOException, SerDeException {
        conf.set("parquet.block.size","134217728");
        JobConf jobConf = new JobConf(conf);
        InputFormat<Void, ArrayWritable> in = new MapredParquetInputFormat();
        ParquetHiveSerDe serde = new ParquetHiveSerDe();

        StringBuilder allColumns = new StringBuilder();
        StringBuilder allColumnTypes = new StringBuilder();
        for (int i = 0; i <= 150; i++) {
            allColumns.append("col");
            allColumnTypes.append("string");
            if (i != 150) {
                allColumns.append(",");
                allColumnTypes.append(":");
            }
        }
        Properties properties = new Properties();
        properties.setProperty("columns", allColumns.toString());
        properties.setProperty("columns.types", allColumnTypes.toString());
        serde.initialize(conf, properties);
        StructObjectInspector inspector = (StructObjectInspector) serde.getObjectInspector();
        System.out.println("**************** " + conf.get("parquet.task.side.metadata"));
        System.out.println("**************** " + conf.get("parquet.block.size"));
        FileInputFormat.addInputPath(jobConf, new Path("/user/hive/warehouse/temp.db/user_tag_new_test/*"));
        InputSplit[] splits = in.getSplits(jobConf, 1);
        for (InputSplit split : splits) {
            // 这里hadoop读取数据，一定要注意hadoop的版本问题， datax里读取parquet，有个小问题困扰了三天
            // 本地代码能正确访问parquet文件，但是datax不能正确访问， 线上是hadoop2.6.0， 而datax是2.7.1， 大版本可能会更新很多代码
            // 把jar替换成和线上版本一致，及解决问题
            // TODO 大数据写代码，一定要注意和集群的版本一致，不然出问题， 你特么都不知道怎么回事
            RecordReader reader = in.getRecordReader(split, jobConf, Reporter.NULL);
            Object K = reader.createKey();
            Object V = reader.createValue();
            int n = 0;
            List<? extends StructField> fields = inspector.getAllStructFieldRefs();
            List<String> recordFields;
            while (reader.next(K, V)) {
                n++;
                recordFields = new ArrayList<String>();
                for (int i = 0; i <= 150; i++) {
                    String field = String.valueOf(inspector.getStructFieldData(V, fields.get(i)));
                    recordFields.add(field);
                }
                if (n <= 2) {
                    System.out.println(StringUtils.join(recordFields.toArray(), ','));
                }
                recordFields.clear();
            }
            System.out.println("======================= " + n);
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

    public static void main(String[] args) throws Exception {
        Configuration conf = HadoopConf.getHAInstance();
        read11(conf);

//        read2(conf);
    }
}