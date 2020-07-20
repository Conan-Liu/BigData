package com.conan.bigdata.hadoop.io;


import com.conan.bigdata.hadoop.util.HadoopConf;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import java.io.IOException;
import java.util.*;

public class HdfsReadExp extends Configured implements Tool {

    public void readText(Configuration conf, String in_path) throws IOException, InterruptedException {
        // 定义任务上下文
        Job job = Job.getInstance(conf, "readText");
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(in_path));
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
    public void read1(Configuration conf, String in_path) throws IOException {
        JobConf jobConf = new JobConf(conf);
        InputFormat<Void, ArrayWritable> in = new MapredParquetInputFormat();
        FileInputFormat.addInputPath(jobConf, new Path(in_path));
        InputSplit[] splits = in.getSplits(jobConf, 1);
        for (InputSplit split : splits) {
            RecordReader reader = in.getRecordReader(split, jobConf, Reporter.NULL);
            Object K = reader.createKey();
            Object V = reader.createValue();
            int n = 0;
            while (reader.next(K, V)) {
                n++;
                if (n <= 200) {
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
    public void read11(Configuration conf, String in_path) throws IOException, SerDeException {
        // conf.set("parquet.block.size", "134217728");
        JobConf jobConf = new JobConf(conf);
        // InputFormat<Void, ArrayWritable> in = new MapredParquetInputFormat();
        MapredParquetInputFormat in = new MapredParquetInputFormat();
        ParquetHiveSerDe serde = new ParquetHiveSerDe();

        StringBuilder allColumns = new StringBuilder();
        StringBuilder allColumnTypes = new StringBuilder();
        for (int i = 0; i <= 7; i++) {
            allColumns.append("col");
            allColumnTypes.append("string");
            if (i != 7) {
                allColumns.append(",");
                allColumnTypes.append(":");
            }
        }
        Properties properties = new Properties();
        properties.setProperty("columns", allColumns.toString());
        properties.setProperty("columns.types", allColumnTypes.toString());
        serde.initialize(conf, properties);
        StructObjectInspector inspector = (StructObjectInspector) serde.getObjectInspector();
        FileInputFormat.addInputPath(jobConf, new Path(in_path));
        InputSplit[] splits = in.getSplits(jobConf, 1);
        for (InputSplit split : splits) {
            // 一定要注意和集群的版本一致，不然容易出问题
            RecordReader reader = in.getRecordReader(split, jobConf, Reporter.NULL);
            Object K = reader.createKey();
            Object V = reader.createValue();
            int n = 0;
            List<? extends StructField> fields = inspector.getAllStructFieldRefs();
            List<String> recordFields = new ArrayList<>();
            while (reader.next(K, V)) {
                n++;
                for (int i = 0; i <= 7; i++) {
                    String field = String.valueOf(inspector.getStructFieldData(V, fields.get(i)));
                    recordFields.add(field);
                }
                if (n <= 200) {
                    System.out.println(StringUtils.join(recordFields.toArray(), ','));
                }
                recordFields.clear();
            }
            System.out.println("======================= " + n);
            reader.close();
        }
    }

    // 新版 InputFormat 读取 Parquet ParquetInputFormat
    public void readParquetWithNewAPI(Configuration conf, String in_path) throws IOException, InterruptedException {
        Job job = Job.getInstance(conf, "readParquetWithNewAPI");
        org.apache.hadoop.mapreduce.InputFormat<Void, Group> in = new ParquetInputFormat<>();
        ParquetInputFormat.addInputPath(job, new Path(in_path));
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
    public void read3(Configuration conf, String in_path) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream in = fs.open(new Path(in_path));
        IOUtils.copyBytes(in, System.out, 4096, false);
        // 重定向数据流的指针到开始， seek 很耗资源， 最好不用
        in.seek(0);
        IOUtils.copyBytes(in, System.out, 4096, false);
        IOUtils.closeStream(in);
    }

    private Set<String> getAllFileSets(Configuration conf, String in_path) throws IOException {
        Set<String> fileSets = new HashSet<>();
        FileSystem hdfs = FileSystem.get(conf);
        Path p = new Path(in_path);
        // 路径中是否有通配符
        if (in_path.contains("*") || in_path.contains("?")) {
            FileStatus[] fileStatuses = hdfs.globStatus(p);
            for (FileStatus f : fileStatuses) {
                if (f.isFile()) {
                    fileSets.add(f.getPath().toString());
                }
            }
        }else{
            FileStatus[] fileStatuses=hdfs.listStatus(p);
        }
        return fileSets;
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = HadoopConf.getHAInstance();
        int result = ToolRunner.run(conf, new HdfsReadExp(), args);
        System.exit(result);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        String in_path = otherArgs[0];
        read11(conf, in_path);
        return 0;
    }
}