package com.conan.bigdata.hadoop.io;

import com.conan.bigdata.hadoop.util.HadoopConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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

    public static class ParquetReaderMapper extends Mapper<Void, Group, IntWritable, Text> {
        private IntWritable K = new IntWritable(1);
        private Text V = new Text();

        @Override
        protected void map(Void key, Group value, Context context) throws IOException, InterruptedException {
            int cityId = value.getInteger("cityid", 0);
            String cityName = value.getString("name", 0);
            int provinceId = value.getInteger("provinceid", 0);
            String province = value.getString("province", 0);
            K.set(cityId);
            V.set(cityId + "," + cityName + "," + provinceId + "," + province);
            context.write(K, V);
        }
    }

    public static class ParquetReaderReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
        private Text V = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                V.set(val);
                context.write(NullWritable.get(), V);
            }
        }
    }

    public static class ParquetReadSupport extends DelegatingReadSupport<Group> {
        public ParquetReadSupport() {
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
        /**
         *  optional  代表可选的值，  required 代表必选的值， 也就是说该字段的值， 可不可以为null
         *  binary    字符串形式
         */
        String readSchema = "message example{\n" +
                "optional int32 cityid;\n" +
                "optional binary name (UTF8);\n" +
                "optional int32 provinceid;\n" +
                "optional binary province (UTF8);\n" +
                "}";
        conf.set(GroupReadSupport.PARQUET_READ_SCHEMA, readSchema);
        Path out=new Path(OUT_PATH);
        FileSystem fs=FileSystem.get(conf);
        if (fs.exists(out)) {
            fs.delete(out,true);
        }

        Job job = Job.getInstance(conf);
        job.setJarByClass(ParquetReaderMR.class);
        job.setJobName(ParquetReaderMR.class.getName());

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(ParquetReaderMapper.class);
        job.setReducerClass(ParquetReaderReducer.class);

        job.setInputFormatClass(ParquetInputFormat.class);
        ParquetInputFormat.setReadSupportClass(job, GroupReadSupport.class);
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