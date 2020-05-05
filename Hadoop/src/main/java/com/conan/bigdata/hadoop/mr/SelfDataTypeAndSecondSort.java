package com.conan.bigdata.hadoop.mr;

import com.conan.bigdata.hadoop.basic.WritableComparableExp;
import com.conan.bigdata.hadoop.basic.WritableExp;
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
 * 自定义数据类型，包括 K，V
 * 自定义 K 实现二次排序
 * 由于自定义了 K，为了能正确的分区和分组，可能需要自定义分区类和分组类
 * <p>
 * 该类同时实现自定义数据类型和二次排序的功能
 * <p>
 * 例子：
 * 数据格式
 * cityid,id   name,age,sex
 * 1,1         liu,11,male
 * 1,3         a,10,male
 * 2,2         li,100,female
 * 2,1         b,-10,male
 * 把上述文本数据，解析为<{@link WritableComparableExp},{@link WritableExp}>
 */
public class SelfDataTypeAndSecondSort extends Configured implements Tool {

    private static final String INPUT_PATH = "/user/mw/mr/SelfDataTypeAndSecondSort/in";
    private static final String OUTPUT_PATH = "/user/mw/mr/SelfDataTypeAndSecondSort/out";

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int result = ToolRunner.run(conf, new SelfDataTypeAndSecondSort(), args);
        System.exit(result);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
        }
        Path out = new Path(OUTPUT_PATH);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(out)) {
            fs.delete(out, true);
        }
        Job job = Job.getInstance(conf);
        job.setJobName("SelfDataTypeAndSecondSort");
        job.setJarByClass(SelfDataTypeAndSecondSort.class);
        job.setMapperClass(SelfDataTypeAndSecondSortMapper.class);
        job.setPartitionerClass(SelfDataTypeAndSecondSortPartitioner.class);
        job.setGroupingComparatorClass(SelfDataTypeAndSecondSortGroupingComparator.class);
        job.setReducerClass(SelfDataTypeAndSecondSortReducer.class);
        // 这个MR是按第一二字段排序的， 但是是分区内排序， 不是全局排序
        // 所以输出的结果如果是多个reduce，那么显然是不能做到所有数据都有序
        // 设置成 1 个可以保证全部数据有序， 但是如果数据量大， 1 个reduce压力大
        job.setNumReduceTasks(2);
        // 因为map和reduce输出的K V类型不一样，所以需要单独指定
        job.setMapOutputKeyClass(WritableComparableExp.class);
        job.setMapOutputValueClass(WritableExp.class);
        job.setOutputKeyClass(WritableComparableExp.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileOutputFormat.setOutputPath(job, out);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class SelfDataTypeAndSecondSortMapper extends Mapper<LongWritable, Text, WritableComparableExp, WritableExp> {
        private WritableComparableExp K = new WritableComparableExp();
        private WritableExp V = new WritableExp();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\\s+");
            String[] ks = splits[0].split(",");
            String[] vs = splits[1].split(",");
            K.set(Integer.parseInt(ks[0]), Integer.parseInt(ks[1]));
            V.set(vs[0], Integer.parseInt(vs[1]), vs[2]);
            context.write(K, V);
        }
    }

    // 二次排序需要自定义分区类， 第一个字段相同的在一个节点上， 才能完整比较， 接下来才能比较第二个字段
    public static class SelfDataTypeAndSecondSortPartitioner extends Partitioner<WritableComparableExp, WritableExp> {
        @Override
        public int getPartition(WritableComparableExp K, WritableExp V, int numPartitions) {
            // 注意， 返回的值是对应的分区id， 一定要是正数才行， 排序的数字可能是负数， 所以需要取绝对值
            return Math.abs(K.getFirst() * 127) % numPartitions;
        }
    }

    /**
     * 自定义分组函数，实现自定义K的比较，确定分组，WritableComparator 继承自 RawComparator
     * 自定义分组就是实现排序的方法
     * return 0  表示的是key为同一组
     * return 1  基本上可以认为升序
     * return -1 基本上可以认为是降序
     * 具体升序降序是要看代码逻辑，以上仅表示通常情况
     */
    public static class SelfDataTypeAndSecondSortGroupingComparator extends WritableComparator {
        protected SelfDataTypeAndSecondSortGroupingComparator() {
            super(WritableComparableExp.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            // WritableComparableExp的两个属性全部相等才认为是这两个对象相等
            WritableComparableExp pair1 = (WritableComparableExp) a;
            WritableComparableExp pair2 = (WritableComparableExp) b;
            if (pair1.getFirst() != pair2.getFirst()) {
                return pair1.getFirst() > pair2.getFirst() ? 1 : -1;
            } else if (pair1.getSecond() != pair2.getSecond()) {
                return pair2.getSecond() > pair2.getSecond() ? 1 : -1;
            } else {
                return 0;
            }
        }
    }

    // 分组函数，可以直接继承自 RawComparator
    // 实现了字节可以比较，速度较快，代码编写难度更高
    public static class SelfDataTypeAndSecondSortGroupingComparator1 implements RawComparator {

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return 1;
        }

        @Override
        public int compare(Object o1, Object o2) {
            return 1;
        }
    }

    public static class SelfDataTypeAndSecondSortReducer extends Reducer<WritableComparableExp, WritableExp, WritableComparableExp, Text> {

        private StringBuilder sb = new StringBuilder();
        private Text V = new Text();

        @Override
        protected void reduce(WritableComparableExp key, Iterable<WritableExp> values, Context context) throws IOException, InterruptedException {
            sb.delete(0, sb.length());
            for (WritableExp val : values) {
                sb.append(val.toString());
            }
            V.set(sb.toString());
            context.write(key, V);
        }
    }
}