package com.conan.bigdata.hadoop.io;

import com.conan.bigdata.hadoop.util.HadoopConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.api.DelegatingReadSupport;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import java.io.IOException;

/**
 * Created by Administrator on 2018/9/6.
 * <p>
 * https://blog.csdn.net/csdnmrliu/article/details/86505386
 */
public class ParquetReaderMR extends Configured implements Tool {

    private static final String IN_PATH = "/user/hadoop/parquetreadermr/in/";
    private static final String OUT_PATH = "/user/hadoop/parquetreadermr/out/";

    private static class ParquetReaderMapper extends Mapper<Void, Group, Text, IntWritable> {
        private IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void map(Void key, Group value, Context context) throws IOException, InterruptedException {
            String province = value.getString("province", 0);
            word.set(province);
            context.write(word, one);
        }
    }

    private static class ParquetReaderReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    private static class ParquetReadSupport extends DelegatingReadSupport<Group>{
        public ParquetReadSupport(){
            super(new GroupReadSupport());
        }

        @Override
        public ReadContext init(InitContext context) {
            return super.init(context);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String readSchema="message example{\n" +
                "required binary province;\n" +
                "}";
        conf.set(ReadSupport.PARQUET_READ_SCHEMA, readSchema);

        Job job = Job.getInstance(conf);
        job.setJarByClass(ParquetReaderMR.class);
        job.setJobName(ParquetReaderMR.class.getName());

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(ParquetReaderMapper.class);
        job.setReducerClass(ParquetReaderReducer.class);

        job.setInputFormatClass(ParquetInputFormat.class);
        ParquetInputFormat.setReadSupportClass(job,ParquetReadSupport.class);
        ParquetInputFormat.addInputPath(job, new Path(IN_PATH));

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));

        if (job.waitForCompletion(true))
            return 0;
        else
            return 1;
    }

    public static void main(String[] args) {
        try {
            int result = ToolRunner.run(HadoopConf.getInstance(), new ParquetReaderMR(), args);
            System.exit(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}