package com.conan.bigdata.hadoop.mr;

import com.conan.bigdata.hadoop.util.HadoopConf;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.Comparator;
import java.util.TreeSet;

/**
 * Created by Administrator on 2019/6/20.
 * <p>
 * 使用TreeSet的排序功能实现TopN
 */
public class TopN1 extends Configured implements Tool {

    private static class TopN1Mapper extends Mapper<LongWritable, Text, LongWritable, NullWritable> {
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
                if (topn.size() > 3) {
                    topn.pollLast();
                }
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

    private static class TopN1Reducer extends Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {
        // 默认升序排, 需要重定义排序方法，降序取TopN名
        private TreeSet<Long> topn = new TreeSet<>(new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                return o1 > o2 ? -1 : 1;
            }
        });

        @Override
        protected void reduce(LongWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            for (NullWritable w : values) {
                topn.add(key.get());
                if (topn.size() > 3) {
                    topn.pollLast();
                }
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

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        // 查看 HadoopConf 类运行时， 所在的jar包
        String path = URLDecoder.decode(HadoopConf.class.getProtectionDomain().getCodeSource().getLocation().getFile(), "UTF-8");
        System.out.println(path);
        Job job = Job.getInstance(conf);
        job.setJobName(TopN1.class.getName());
        job.setJarByClass(TopN1.class);
        job.setMapperClass(TopN1Mapper.class);
        job.setReducerClass(TopN1Reducer.class);
        // 想做到全部数据的TopN， 只能是一个Reduce去计算， 才能保证
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(NullWritable.class);

        // FileInputFormat FileOutputFormat 这两个是文件操作的抽象类， 不能直接使用该类作为输入输出类
        // 必须使用这两个抽象类的具体子类
        job.setInputFormatClass(TextInputFormat.class);
        // job.setOutputFormatClass(FileOutputFormat.class); 这是错的， 不能直接使用FileOutputFormat
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("/user/hdfs/temp/in/"));
        FileOutputFormat.setOutputPath(job, new Path("/user/hdfs/temp/out/"));
        boolean isSuccess = job.waitForCompletion(true);
        return isSuccess ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        // 这个地方要注意了， HadoopConf 这个类是用来加载配置的， 但是因为名字比较广泛， 很容易打包的时候， 其它的jar包里面
        // 有这个类， 导致， 我在代码里面修改这个类， 搞了一头的汗， 运行的时候并没有效果， 只因为自己之前打包的时候， 把这个类打包
        // 放线上了， 且这个jar包包含这个类， classpath 优先级高， 所以，每次都是加载线上老的jar包， 我自己打包的工作jar包， 一直不生效
        // 可以打印这个类的全路径和这个类运行时所在的jar包，查看具体加载的类， 定位错误
        int result = ToolRunner.run(HadoopConf.getInstance(), new TopN1(), args);
        System.exit(result);
    }
}