package com.conan.bigdata.hadoop.mr;

import com.conan.bigdata.hadoop.basic.IntPair;
import com.conan.bigdata.hadoop.util.HadoopConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 二次排序
         */
public class SecondSort extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(HadoopConf.getHAInstance(), new SecondSort(), args);
        System.exit(result);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
        }
        Path out = new Path("/user/hive/temp/out");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(out)) {
            fs.delete(out, true);
        }
        Job job = Job.getInstance(conf);
        job.setJobName("SecondSort");
        job.setJarByClass(SecondSort.class);
        job.setMapperClass(SecondSortMapper.class);
        job.setPartitionerClass(SecondSortPartitioner.class);
        job.setGroupingComparatorClass(SecondSortGroupingComparator.class);
        job.setReducerClass(SecondSortReducer.class);
        // 这个MR是按第一二字段排序的， 但是是分区内排序， 不是全局排序
        // 所以输出的结果如果是多个reduce，那么显然是不能做到所有数据都有序
        // 设置成 1 个可以保证全部数据有序， 但是如果数据量大， 1 个reduce压力大
        job.setNumReduceTasks(2);
        // 因为map和reduce输出的K V类型不一样，所以需要单独指定
        job.setMapOutputKeyClass(IntPair.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path("/tmp/test/"));
        FileOutputFormat.setOutputPath(job, out);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class SecondSortMapper extends Mapper<LongWritable, Text, IntPair, NullWritable> {
        private IntPair pair = new IntPair();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\\s+");
            if (splits.length == 2) {
                pair.set(Integer.parseInt(splits[0]), Integer.parseInt(splits[1]));
                context.write(pair, NullWritable.get());
            }
        }
    }

    // 二次排序需要自定义分区类， 第一个字段相同的在一个节点上， 才能完整比较， 接下来才能比较第二个字段
    public static class SecondSortPartitioner extends Partitioner<IntPair, NullWritable> {
        @Override
        public int getPartition(IntPair K, NullWritable V, int numPartitions) {
            // 注意， 返回的值是对应的分区id， 一定要是正数才行， 排序的数字可能是负数， 所以需要取绝对值
            return Math.abs(K.getFirst() * 127) % numPartitions;
        }
    }

    // 自定义分组函数, WritableComparator 继承自 RawComparator
    public static class SecondSortGroupingComparator extends WritableComparator {
        protected SecondSortGroupingComparator() {
            super(IntPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            IntPair pair1 = (IntPair) a;
            IntPair pair2 = (IntPair) b;
            int f1 = pair1.getFirst();
            int f2 = pair2.getFirst();
            return f1 == f2 ? 0 : (f1 > f2 ? 1 : -1);
        }
    }

    // 分组函数，可以直接继承自 RawComparator
    public static class SecondSortGroupingComparator1 implements RawComparator{

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return 1;
        }

        @Override
        public int compare(Object o1, Object o2) {
            return 1;
        }
    }

    public static class SecondSortReducer extends Reducer<IntPair, NullWritable, IntWritable, IntWritable> {
        private IntWritable K = new IntWritable();
        private IntWritable V = new IntWritable();

        @Override
        protected void reduce(IntPair key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            for (NullWritable val : values) {
                K.set(key.getFirst());
                V.set(key.getSecond());
                context.write(K, V);
            }
        }
    }
}