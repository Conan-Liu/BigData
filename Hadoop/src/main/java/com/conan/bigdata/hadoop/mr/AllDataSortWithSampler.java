package com.conan.bigdata.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;

/**
 * 1. 先根据抽样算法得到分区文件
 * 2. 然后map处理数据，并根据分区文件生成对应分区
 * 3. 利用框架来自动排序
 * <p>
 * 抽样算法能很大程度上减少数据倾斜造成的问题，数据相对均匀
 */
public class AllDataSortWithSampler extends Configured implements Tool {

    private static final String INPUT_PATH = "/user/mw/mr/alldatasort/in";
    private static final String OUTPUT_PATH = "/user/mw/mr/alldatasort/out";

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int result = ToolRunner.run(conf, new AllDataSortWithSampler(), args);
        System.exit(result);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        job.setJobName("AllDataSortWithSampler");
        job.setJarByClass(AllDataSortWithSampler.class);
        job.setMapperClass(AllDataSortWithSamplerMapper.class);
        job.setReducerClass(AllDataSortWithSamplerReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(4);

        // 进行采样，并把分区的边界值写入分区文件中，路径默认设置为mapreduce.totalorderpartitioner.path
        // 注意这里需要使用操作job对象，所以该有的配置应该提前在配置好，代码顺序不能错，否则报错
        // 如：把FileInputFormat设置job的输入目录放在代码后面，该排序算法则会报job找不到input path
        Path partitionPath = new Path(INPUT_PATH + "_partition");
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionPath);
        job.setPartitionerClass(TotalOrderPartitioner.class);

        InputSampler.Sampler<LongWritable, Text> sampler = new InputSampler.RandomSampler<LongWritable, Text>(0.8, 10000, 4);
        InputSampler.writePartitionFile(job, sampler);

        // 分区文件缓存下来，加速map处理
        URI uri = new URI(partitionPath.toString());
        job.addCacheFile(uri);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private static class AllDataSortWithSamplerMapper extends Mapper<LongWritable, Text, LongWritable, NullWritable> {
        private LongWritable K = new LongWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\\s+");
            for (String s : splits) {
                K.set(Long.parseLong(s));
                context.write(K, NullWritable.get());
            }
        }
    }

    private static class AllDataSortWithSamplerReducer extends Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {
        @Override
        protected void reduce(LongWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }
}