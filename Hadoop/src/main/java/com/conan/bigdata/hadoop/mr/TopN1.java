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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeSet;

/**
 * Created by Administrator on 2019/6/20.
 *
 * 使用TreeSet的排序功能实现TopN
 */
public class TopN1 extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new TopN1(), args);
        System.exit(result);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Job job = Job.getInstance(conf);
        job.setJobName(TopN1.class.getName());
        job.setJarByClass(TopN1.class);
        job.setMapperClass(TopN1Map.class);
        job.setReducerClass(TopN1Reduce.class);
        // 想做到全部数据的TopN， 只能是一个Reduce去计算， 才能保证
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(FileOutputFormat.class);

        FileInputFormat.addInputPaths(job, "/user/hdfs/temp/in/");
        FileOutputFormat.setOutputPath(job, new Path("/user/hdfs/temp/out"));
        boolean isSuccess = job.waitForCompletion(true);
        return isSuccess ? 0 : 1;
    }

    private static class TopN1Map extends Mapper<LongWritable, Text, LongWritable, NullWritable> {
        // 默认升序排, 需要重定义排序方法，降序取TopN名
        private TreeSet<Long> topn = new TreeSet<>(new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                return o1 > o2 ? -1 : 1;
            }
        });

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 不推荐 StringTokenizer
            String[] values = value.toString().split("\\s+");
            for (String val : values) {
                topn.add(Long.parseLong(val));
            }
            if (topn.size() > 3) {
                topn.pollLast();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            LongWritable K = new LongWritable();
            for (Long val : topn) {
                K.set(val);
                context.write(K, NullWritable.get());
            }
        }
    }

    private static class TopN1Reduce extends Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {
        // 默认升序排, 需要重定义排序方法，降序取TopN名
        private TreeSet<Long> topn = new TreeSet<>(new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                return o1 > o2 ? -1 : 1;
            }
        });

        @Override
        protected void reduce(LongWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            topn.add(key.get());
            if (topn.size() > 3) {
                topn.pollLast();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            LongWritable K = new LongWritable();
            for (Long val : topn) {
                K.set(val);
                context.write(K, NullWritable.get());
            }
        }
    }
}