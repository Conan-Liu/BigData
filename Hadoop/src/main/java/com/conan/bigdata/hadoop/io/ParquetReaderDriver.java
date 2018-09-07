package com.conan.bigdata.hadoop.io;

import com.conan.bigdata.hadoop.util.HadoopConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.example.GroupWriteSupport;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by Administrator on 2018/9/6.
 */
public class ParquetReaderDriver extends Configured implements Tool {

    private static final String IN_PATH = "/user/deploy/mr/in/text.txt";
    private static final String OUT_PATH = "/user/deploy/mr/out/";

    public static class ParquetReaderMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer token = new StringTokenizer(line);
            while (token.hasMoreTokens()) {
                word.set(token.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class ParquetReaderReducer extends Reducer<Text, IntWritable, Void, Group> {
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
            Group group = factory.newGroup().append("name", key.toString()).append("age", sum);
            context.write(null, group);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String writeSchema = "message example{\n" +
                "required binary name;\n" +
                "required int32 age;\n" +
                "}";
        conf.set("parquet.example.schema", writeSchema);
        Job job = Job.getInstance(conf);
        job.setJarByClass(ParquetReaderDriver.class);
        job.setJobName(ParquetReaderDriver.class.getName());

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(Group.class);

        job.setMapperClass(ParquetReaderMapper.class);
        job.setReducerClass(ParquetReaderReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(ParquetOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(IN_PATH));
        ParquetOutputFormat.setOutputPath(job, new Path(OUT_PATH));
        ParquetOutputFormat.setWriteSupportClass(job, GroupWriteSupport.class);

        if (job.waitForCompletion(true))
            return 0;
        else
            return 1;
    }

    public static void main(String[] args) {
        try {
            int result = ToolRunner.run(HadoopConf.getInstance(), new ParquetReaderDriver(), args);
            System.exit(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}