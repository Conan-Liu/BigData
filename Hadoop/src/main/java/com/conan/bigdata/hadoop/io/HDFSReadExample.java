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
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class HDFSReadExample {

    public static void readText(Configuration conf) throws IOException, InterruptedException {
        // 定义任务上下文
        Job job = Job.getInstance(conf, "readText");
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path("/user/mw/mr/wordcount/in/*"));
        // 获取文件输入类
        org.apache.hadoop.mapreduce.InputFormat<LongWritable, Text> in = new TextInputFormat();
        // 获取文件的输入分片
        List<org.apache.hadoop.mapreduce.InputSplit> splits = in.getSplits(job);
        // 遍历分片，一行一行读取
        for (org.apache.hadoop.mapreduce.InputSplit split : splits) {
            TaskAttemptContext taskAttemptContext = new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());
            org.apache.hadoop.mapreduce.RecordReader<LongWritable, Text> recordReader = in.createRecordReader(split, taskAttemptContext);
            recordReader.initialize(split, taskAttemptContext);
            int cnt = 0;
            while (recordReader.nextKeyValue()) {
                cnt++;
                /**
                 * 从打印结果来看 {@link TextInputFormat} 返回的key是字节偏移量
                 * 单个文件不同的block上，偏移量是递增的，并不是从0开始重新计算
                 */
                if (cnt <= 10) {
                    System.out.println(recordReader.getCurrentKey().get() + " -- " + recordReader.getCurrentValue().toString());
                }
            }
            recordReader.close();
        }
    }

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
        conf.set("parquet.block.size", "134217728");
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
            // TODO 一定要注意和集群的版本一致，不然容易出问题
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
    public static void readParquetWithNewAPI(Configuration conf) throws IOException, InterruptedException {
        Job job = Job.getInstance(conf, "readParquetWithNewAPI");
        org.apache.hadoop.mapreduce.InputFormat<Void, Group> in = new ParquetInputFormat<>();
        ParquetInputFormat.addInputPath(job, new Path("/user/mw/parquet/"));
        ParquetInputFormat.setReadSupportClass(job, GroupReadSupport.class);
        List<org.apache.hadoop.mapreduce.InputSplit> splits = in.getSplits(job);
        for (org.apache.hadoop.mapreduce.InputSplit split : splits) {
            TaskAttemptContext taskAttemptContext = new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());
            org.apache.hadoop.mapreduce.RecordReader<Void, Group> reader = in.createRecordReader(split, taskAttemptContext);
            reader.initialize(split, taskAttemptContext);
            Group V;
            int cnt = 0;
            while (reader.nextKeyValue()) {
                cnt++;
                if (cnt <= 10) {
                    V = reader.getCurrentValue();
                    int f1 = V.getInteger("f1", 0);
                    String f2 = V.getString("f2", 0);
                    System.out.println(f1 + " -- " + f2);
                }
            }
            System.out.println("行数: " + cnt);
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
//        Configuration conf = HadoopConf.getHAInstance();
        Configuration conf = new Configuration();
        readParquetWithNewAPI(conf);
    }
}