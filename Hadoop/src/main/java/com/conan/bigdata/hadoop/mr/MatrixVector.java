package com.conan.bigdata.hadoop.mr;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * 矩阵向量的乘法
 * 数据逻辑参考百度
 * <p>
 * 该示例是 3 x 3 的矩阵， 向量也是 3
 * 1  2  3
 * 4  5  6
 * 7  8  9
 */
public class MatrixVector extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(HadoopConf.getHAInstance(), new MatrixVector(), args);
        System.exit(result);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Path out = new Path("/user/hive/temp/out");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(out)) {
            fs.delete(out, true);
        }
        Job job = Job.getInstance(conf);
        job.setJobName("MatrixVector");
        job.setJarByClass(MatrixVector.class);
        // 新版的添加缓存文件, # 表示软链， 当访问改文件的时候，指定这个软链名称即可
        job.addCacheFile(new URI("/tmp/test/aaa#symlink"));
        job.setMapperClass(MatrixVectorMapper.class);
        job.setReducerClass(MatrixVectorReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path("/tmp/test/ccc"));
        FileOutputFormat.setOutputPath(job, out);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class MatrixVectorMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        // 这个 K 就是矩阵的行号， 对应列向量的行号
        private IntWritable K = new IntWritable();
        // V 对应矩阵每个元素和列向量每个元素的乘积
        private IntWritable V = new IntWritable();
        private static int i = 0;
        // private final static int[] vector = {2, 3, 4};
        private final static List<Integer> vector = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                // 这个文件名与上面定义的软链保持一致
                BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("symlink")));
                String line;
                while ((line = br.readLine()) != null) {
                    vector.add(Integer.parseInt(line.trim()));
                }
                br.close();
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] vals = value.toString().split("\\s+");
            int j = 0;
            K.set(i);
            for (String val : vals) {
                int result = vector.get(j) * Integer.parseInt(val);
                V.set(result);
                context.write(K, V);
                j++;
            }
            i++;
        }
    }

    public static class MatrixVectorReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private IntWritable V = new IntWritable();

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // 对应行号的数值，相加即得到向量乘积
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            V.set(sum);
            context.write(key, V);
        }
    }
}