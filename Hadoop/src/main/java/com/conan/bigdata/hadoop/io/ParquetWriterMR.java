package com.conan.bigdata.hadoop.io;

import com.conan.bigdata.hadoop.util.HadoopConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.example.GroupWriteSupport;

import java.io.IOException;

/**
 * Created by Administrator on 2018/9/6.
 * <p>
 * https://blog.csdn.net/csdnmrliu/article/details/86505386
 */
public class ParquetWriterMR extends Configured implements Tool {

    private static final String IN_PATH = "/user/hadoop/topn/in/";
    private static final String OUT_PATH = "/user/hadoop/topn/out/";

    public static class ParquetWriterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text K = new Text();
        private IntWritable V = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\\s+");
            for (String s : splits) {
                K.set(s);
                context.write(K, V);
            }
        }
    }

    /**
     * 大部分情况下， Combine 和 Reducer 都可以雷同， 前提是 输入的KV数据类型， 要和输出的KV数据类型一致才行
     * 这里Combine 为了减少数据量， 所以输入输出都是Text, IntWritable
     * 但是Reducer 输出到HDFS上的输出是Void, Group
     * 两者不一样， 所以单独写
     */
    public static class ParquetWriterCombine extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Text K = new Text();
        private IntWritable V = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            V.set(sum);
            context.write(K, V);
        }
    }

    public static class ParquetWriterReducer extends Reducer<Text, IntWritable, Void, Group> {
        private SimpleGroupFactory factory;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            factory = new SimpleGroupFactory(GroupWriteSupport.getSchema(context.getConfiguration()));
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            Group group = factory.newGroup().append("word", key.toString())
                    .append("cnt", sum);
            context.write(null, group);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        /**
         *  optional  代表可选的值，  required 代表必选的值， 也就是说该字段的值， 可不可以为null
         *  binary    字符串形式
         */
        String writeSchema = "message example{\n" +
                "required binary word;\n" +
                "required int32 cnt;\n" +
                "}";
        conf.set(GroupWriteSupport.PARQUET_EXAMPLE_SCHEMA,writeSchema);
        Path out = new Path(OUT_PATH);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(out)) {
            fs.delete(out, true);
        }

        Job job = Job.getInstance(conf);
        job.setJarByClass(ParquetWriterMR.class);
        job.setJobName(ParquetWriterMR.class.getName());

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Group.class);

        job.setMapperClass(ParquetWriterMapper.class);
        job.setCombinerClass(ParquetWriterCombine.class);
        job.setReducerClass(ParquetWriterReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(IN_PATH));

        job.setOutputFormatClass(ParquetOutputFormat.class);
        ParquetOutputFormat.setWriteSupportClass(job,GroupWriteSupport.class);
        ParquetOutputFormat.setOutputPath(job, out);

        if (job.waitForCompletion(true))
            return 0;
        else
            return 1;
    }

    public static void main(String[] args) {
        try {
            int result = ToolRunner.run(HadoopConf.getInstance(), new ParquetWriterMR(), args);
            System.exit(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}