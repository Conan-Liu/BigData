package com.conan.bigdata.hbase.mr;

import com.conan.bigdata.hbase.common.Constant;
import com.conan.bigdata.hbase.util.HBaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.api.ReadSupport;

import java.io.IOException;

public class LoadDataToHbaseDriver extends Configured implements Tool {

    public static void main(String[] args) {
        try {
            int isSuccess = ToolRunner.run(HBaseUtils.getHBaseConf(), new LoadDataToHbaseDriver(), args);
            if (isSuccess == 0) {
                System.out.println(Constant.JOB_NAME + " is successfully completed...");
            } else {
                System.out.println(Constant.JOB_NAME + " failed...");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        // 这里的readSchema是用来读取指定的字段，指定的字段不存在会报错
        // 如果不指定，则使用Parquet文件本身的schmea来读取所有的字段，可查看GroupReadSupport类
        String readSchema = "message example {\n" +
                "required binary id;\n" +
                "required binary mw_id\n" +
                "}";
        conf.set(ReadSupport.PARQUET_READ_SCHEMA, readSchema);

        Job job = Job.getInstance(conf, Constant.JOB_NAME);
        job.setJarByClass(LoadDataToHbaseDriver.class);
        // 下面这个命令是把第三方jar包添加到hadoop的任务中，避免ClassNotFoundException, 这个jar包是在hdfs上, 而且必须是指定到某个jar包， 不能文件夹
        // job.addArchiveToClassPath(new Path("/user/hadoop/libs/parquet-hadoop.jar"));
        // job.addArchiveToClassPath(new Path(Constant.EXT_LIBS));
        createHadoopClassPath(job);
        System.out.println("temp jars is : " + conf.get("tmpjars"));

        HBaseUtils.deleteDir(Constant.OUTPUT_PATH);

        job.setMapperClass(LoadDataToHbaseMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        //in/out format
        job.setInputFormatClass(ParquetInputFormat.class);
        ParquetInputFormat.setReadSupportClass(job, GroupReadSupport.class);
        //input path
        Path inPath = new Path(Constant.IN_PATH);
        FileInputFormat.addInputPath(job, inPath);

        job.setOutputFormatClass(HFileOutputFormat2.class);

        //output path
        Path outPath = new Path(Constant.OUTPUT_PATH);
        FileOutputFormat.setOutputPath(job, outPath);


        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf(Constant.TABLE_NAME);
        Table table = connection.getTable(tableName);

        // 这里相当于配置Reducer，所以该MR任务涉及reduce的配置可以省略
        // 该配置指定了输出KV对 <ImmutableBytesWritable, KeyValue>
        HFileOutputFormat2.configureIncrementalLoad(job, table, connection.getRegionLocator(tableName));

        boolean isSuccess=job.waitForCompletion(true);
        if (isSuccess) {
            // 文件权限可能不够，修改权限
            FsShell shell = new FsShell(conf);
            try {
                shell.run(new String[]{"-chmod", "-R", "777", args[1]});
            } catch (Exception e) {
                System.out.println("Couldnt change the file permissions...");
                throw new IOException(e);
            }
            //load data
            LoadIncrementalHFiles loadHfiles = new LoadIncrementalHFiles(conf);
            loadHfiles.doBulkLoad(outPath, admin, table, connection.getRegionLocator(tableName));
            System.out.println("Bulk Load Completed ...");
            return 0;
        } else {
            System.out.println("Bulk Load Error ...");
            return 1;
        }

    }


    public static void createHadoopClassPath(Job job) throws IOException {
        FileSystem fs = FileSystem.get(HBaseUtils.getHBaseConf());
        FileStatus[] listFiles = fs.listStatus(new Path(Constant.EXT_LIBS));
        StringBuilder sb = new StringBuilder();
        for (FileStatus file : listFiles) {
            job.addArchiveToClassPath(file.getPath());
        }
    }

}
