package com.conan.bigdata.hadoop.mr;

import com.conan.bigdata.hadoop.util.HadoopConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.hadoop.ParquetOutputFormat;

import java.io.IOException;

/**
 * hadoop 任务输出到多个目录里面, 默认是一个输出目录
 */
public class MultipleOutputDir extends Configured implements Tool {

    private static final String INPUT_PATH = "/user/mw/mr/MultipleOutputDir/in";
    private static final String OUTPUT_PATH = "/user/mw/mr/MultipleOutputDir/out";

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "MultipleOutputDir");
        Path out = new Path(OUTPUT_PATH);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(out)) {
            fs.delete(out, true);
        }
        job.setJarByClass(MultipleOutputDir.class);
        job.setMapperClass(MultipleOutputDirMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(TextInputFormat.class);
        // 去除输出目录下， 几个part- 开头的空文件
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        // job.setOutputFormatClass(TextOutputFormat.class);

        // MultipleOutputs.write有两个重载的方法，如果想不同的输出对应不同的OutputFormat，可以使用如下操作
        // 如下演示的第二种方式： 这里定义了两个output方式，可以根绝name来调用
        // MultipleOutputs.addNamedOutput(job,"output1",TextOutputFormat.class,Text.class,Text.class);
        // MultipleOutputs.addNamedOutput(job,"output2",ParquetOutputFormat.class,Text.class,Text.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, out);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        int result = ToolRunner.run(conf, new MultipleOutputDir(), args);
        System.exit(result);
    }

    public static class MultipleOutputDirMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private MultipleOutputs<NullWritable, Text> multipleOutputs = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<>(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 这里有一个重载方法， 注意，两个方法功能一样，但是有区别，推荐用第一种方式即可
            multipleOutputs.write(NullWritable.get(), value, getFileName(value));
            // 演示第二个
            multipleOutputs.write("output1",NullWritable.get(),value,getFileName(value));
        }

        private String getFileName(Text value) {
            String country[] = value.toString().split("\\s+");
            // 如果没有这个 / , 则文件名为 china-m-00000
            return country[0].toLowerCase() + "/file";
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 一定要关闭这个，否则会没有输出
            if (multipleOutputs != null)
                multipleOutputs.close();
        }
    }

}