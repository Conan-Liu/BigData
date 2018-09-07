package com.conan.bigdata.hbase.mr;

import com.conan.bigdata.hbase.common.CONSTANT;
import com.conan.bigdata.hbase.util.HBaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.api.ReadSupport;

import java.io.IOException;


/**
 * Created by admin on 2017/1/5.
 */
public class LoadDataToHbaseDriver extends Configured implements Tool {

    public static void main(String[] args) {
        try {
            int isSuccess = ToolRunner.run(HBaseUtils.getHBaseConf(), new LoadDataToHbaseDriver(), args);
            if (isSuccess == 0) {
                System.out.println(CONSTANT.JOB_NAME + " is successfully completed...");
            } else {
                System.out.println(CONSTANT.JOB_NAME + " failed...");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String readSchema = "message example {\n" +
                "required binary id;\n" +
                "required binary mw_id\n" +
                "}";
        conf.set(ReadSupport.PARQUET_READ_SCHEMA, readSchema);

        Job job = Job.getInstance(conf, CONSTANT.JOB_NAME);
        job.setJarByClass(LoadDataToHbaseDriver.class);
        // 下面这个命令是把第三方jar包添加到hadoop的任务中，避免ClassNotFoundException, 这个jar包是在hdfs上, 而且必须是指定到某个jar包， 不能文件夹
        //job.addArchiveToClassPath(new Path("/user/hadoop/libs/parquet-hadoop.jar"));
//        job.addArchiveToClassPath(new Path(CONSTANT.EXT_LIBS));
        createHadoopClassPath(job);
        System.out.println("temp jars is : " + conf.get("tmpjars"));

        HBaseUtils.deleteDir(CONSTANT.OUTPUT_PATH);
        job.setMapperClass(TestParquetMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        //in/out format
        job.setInputFormatClass(ParquetInputFormat.class);
//        ParquetInputFormat.setReadSupportClass(job, TestReadSupport.class);
        //input path
        Path inPath = new Path(CONSTANT.IN_PATH);
//        FileInputFormat.addInputPath(job, inPath);
        ParquetInputFormat.addInputPath(job, inPath);

//        job.setOutputFormatClass(HFileOutputFormat2.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        //output path
        Path outPath = new Path(CONSTANT.OUTPUT_PATH);
        FileOutputFormat.setOutputPath(job, outPath);
        if (job.waitForCompletion(true))
            return 0;
        else
            return 1;

//        Connection connection = ConnectionFactory.createConnection(conf);
//        TableName tableName = TableName.valueOf(CONSTANT.TABLE_NAME);
//        Table table = connection.getTable(tableName);
//        HFileOutputFormat2.configureIncrementalLoad(job, table, connection.getRegionLocator(tableName));
//
//        if (job.waitForCompletion(true)) {
//            FsShell shell = new FsShell(conf);
//            try {
//                shell.run(new String[]{"-chmod", "-R", "777", args[1]});
//            } catch (Exception e) {
//                System.out.println("Couldnt change the file permissions...");
//                throw new IOException(e);
//            }
//            //load data
//            LoadIncrementalHFiles loadHfiles = new LoadIncrementalHFiles(conf);
//            loadHfiles.doBulkLoad(outPath, (HTable) table);
//            System.out.println("Bulk Load Completed...");
//            return 0;
//        } else {
//            return 1;
//        }

    }


    public static void createHadoopClassPath(Job job) throws IOException {
        FileSystem fs = FileSystem.get(HBaseUtils.getHBaseConf());
        FileStatus[] listFiles = fs.listStatus(new Path(CONSTANT.EXT_LIBS));
        StringBuilder sb = new StringBuilder();
        for (FileStatus file : listFiles) {
            job.addArchiveToClassPath(file.getPath());
        }
    }

}
