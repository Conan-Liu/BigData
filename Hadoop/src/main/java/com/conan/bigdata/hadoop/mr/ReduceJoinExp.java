package com.conan.bigdata.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReduceJoinExp extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(ReduceJoinExp.class);

    private static final String INPUT_PATH = "/user/mw/mr/ReduceJoinExp/in";
    private static final String OUTPUT_PATH = "/user/mw/mr/ReduceJoinExp/out";

    public static class ReduceJoinExpMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text K = new Text();
        private Text V = new Text();
        private String fileName;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\\s+");
            if ("join1".equals(fileName)) {
                K.set(splits[0]);
                V.set("1-" + splits[1]);
            } else {
                K.set(splits[0]);
                V.set("2-" + splits[1]);
            }
            LOG.warn("map = {},{}", K, V);
            context.write(K, V);
        }
    }

    public static class ReduceJoinExpReducer extends Reducer<Text, Text, Text, Text> {
        private Text V = new Text();
        private List<String> vv = new ArrayList<>(16);

        /**
         * 注意这里的reduce方法的 V 参数是一个Iterable，普通java程序是可以多次迭代
         * 但是这里确只能迭代一次，不能迭代两次及以上，否则数据显示全都一样
         * Iterator的next()方法每次返回的是同一个对象，next()只是修改了Writable对象的值，而不是重新返回一个新的Writable对象
         * 如果要重复使用，必须clone对象，可以new 一个，也可以直接调用toString方法
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String join1 = null;
            for (Text val : values) {
                if (val.toString().startsWith("1-")) {
                    join1 = val.toString();
                } else {
                    // 注意这里需要调用toString才能确保不发生数据一样的问题
                    vv.add(val.toString());
                }
            }
            for (String value : vv) {
                V.set(join1 + "|" + value);
                context.write(key, V);
            }
            vv.clear();

            // 两个for变量，这个例子不会打印数据， 因为Iterable只能遍历一次
            // for (Text val : values) {
            //     System.out.println("2 == " + val);
            // }

        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "ReduceJoinExp");
        job.setJarByClass(ReduceJoinExp.class);
        job.setMapperClass(ReduceJoinExpMapper.class);
        job.setReducerClass(ReduceJoinExpReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int result = ToolRunner.run(conf, new ReduceJoinExp(), args);
        System.exit(result);
    }
}