package com.conan.bigdata.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
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
import java.util.HashMap;
import java.util.Map;

/**
 * 演示 map join
 */

public class MapJoinExp extends Configured implements Tool {

    private static final String INPUT_PATH = "/user/mw/mr/MapJoinExp/in";
    private static final String OUTPUT_PATH = "/user/mw/mr/MapJoinExp/out";
    private static final String CACHE_PATH = "/user/mw/mr/MapJoinExp/join1";

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int result = ToolRunner.run(conf, new MapJoinExp(), args);
        System.exit(result);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Path out = new Path(OUTPUT_PATH);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(out)) {
            fs.delete(out, true);
        }
        Job job = Job.getInstance(conf);
        job.setJobName("MapJoinExp");
        job.setJarByClass(MapJoinExp.class);
        job.setMapperClass(MapReduceJoinMapper.class);
        job.setNumReduceTasks(0);
        URI cacheUri = new URI(CACHE_PATH+"#symlink");
        job.addCacheFile(cacheUri);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, out);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private static class MapReduceJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Map<String, String> cache = new HashMap<>();
        private Text K = new Text();
        private Text V = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                System.out.println("读取cache");
                // 一定要用软链，否则报 FileNotFoundException: /user/mw/mr/MapJoinExp/join1
                BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("symlink")));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] ss = line.split("\\s+");
                    cache.put(ss[0], ss[1]);
                }
                br.close();
            }
            System.out.println("读取完毕");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\\s+");
            K.set(splits[0]);
            V.set(cache.get(splits[0]) + "|" + splits[1]);
            context.write(K, V);
        }
    }

}
