package com.conan.bigdata.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(WordCount.class);

    private static final String INPUT_PATH = "/user/mw/mr/wordcount/in";
    private static final String OUTPUT_PATH = "/user/mw/mr/wordcount/out";

    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        // 这种全局定义变量的方式， 可以重复利用现有对象， 减少new的对象， 减少GC开销
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.getCounter("MyGroup", "element1").increment(1);
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
            // 注意这是在map端的日志，所以只能在map的日志中找到
            // 输出到 stdout
            System.out.println("map stdout.......");
            // 输出到 syslog
            LOG.info("map info.......");
            LOG.error("map error.......");
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            // reduce端的日志，只能在reduce的日志中找到
            // 输出到 stdout
            System.out.println("reduce stdout.......");
            // 输出到 syslog
            LOG.info("reduce info.......");
            LOG.error("reduce error.......");
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class WordCountSort extends Text.Comparator{
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            // 默认升序，可以不屑
            return super.compare(b1, s1, l1, b2, s2, l2);
            // 降序
            // return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        // 如果权限不对， 设置这个指定用户名去访问
        // System.setProperty("HADOOP_USER_NAME", "bdata");
        Configuration conf = getConf();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // 自定义配置参数
        System.out.println("self-conf:" + conf.get("self"));
        LOG.warn("self-conf info {}", conf.get("self"));
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            // System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count");
        // 本地提交到集群，需要设置这个，否则报WordCount$WordCountMapper not found，集群上执行hadoop jar提交则不需要这条代码
        // 因为任务的执行，需要把jar包传递到各个节点上
        // job.setJar("/Users/mw/project/github/BigData/Hadoop/target/hadoop-1.0-SNAPSHOT.jar");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // shuffle默认升序，排序设置
        job.setSortComparatorClass(WordCountSort.class);

        // 可以循环添加多个输入路径
        // for (int i = 0; i < otherArgs.length - 1; ++i) {
        //    FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        // }
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        // 输出压缩
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 注意文件副本数和文件块大小都是客户端参数，所以程序里面一定要注意，如果是默认参数，则会覆盖集群上的配置
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // 通过 -D 传参数
        // conf.set("mapreduce.job.ubertask.enable","true");
        // 不要生成success文件，默认生成
        conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
        conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2");
        // 注意看：直接通过new Configuration()方法传入配置，默认是使用的单机模式执行任务，由jobid可知
        // 传参 /Users/mw/bigdata/hadoop/hadoop-2.6.0-cdh5.10.0-single/test/in /Users/mw/bigdata/hadoop/hadoop-2.6.0-cdh5.10.0-single/test/out
        // 可以通过添加*-site.xml文件，来覆盖默认的配置，不使用本地文件系统，而是使用hdfs
        int result = ToolRunner.run(conf, new WordCount(), args);

        // 利用DistributedFileSystem提交到集群上，这里还是单机模式，只不过文件系统不再是本地文件系统，而是hdfs，可以查看jobid
        // 需要引入集群的配置文件才能找到RM，并提交任务到集群上，还需要setJar来指定jar包，不利于代码迭代，直接打包人集群上用命令执行完全ok
        // 本地提交集群RM上执行的方式，不再考虑，浪费时间
        // int result = ToolRunner.run(HadoopConf.getInstance(), new WordCount(), args);

        System.exit(result);
    }
}