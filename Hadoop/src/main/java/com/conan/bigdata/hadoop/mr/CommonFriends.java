package com.conan.bigdata.hadoop.mr;

import com.conan.bigdata.hadoop.util.HadoopConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

import java.io.IOException;
import java.util.Arrays;

/**
 * 计算下面用户的共同好友， 冒号前是一个用户， 冒号后是该用户的好友
 * Spark 版本参考 com.conan.bigdata.spark.scala.CommonFriends
 * 数据格式如下：
 * A:B,C,D,F,E,O
 * B:A,C,E,K
 * C:F,A,D,I
 * D:A,E,F,L
 * E:B,C,D,M,L
 * F:A,B,C,D,E,O,M
 * G:A,C,D,E,F
 * H:A,C,D,E,O
 * I:A,O
 * J:B,O
 * K:A,C,D
 * L:D,E,F
 * M:E,F,G
 * O:A,H,I,J
 */
public class CommonFriends extends Configured implements Tool {

    private static final String IN_PATH = "/user/hive/temp/in";
    private static final String OUT_PATH1 = "/user/hive/temp/out1";
    private static final String OUT_PATH2 = "/user/hive/temp/out2";


    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        // 第一个job
        Path out1 = new Path(OUT_PATH1);
        if (fs.exists(out1)) {
            fs.delete(out1, true);
        }
        Job job1 = Job.getInstance(conf);
        job1.setJobName(TopN.class.getName() + "-1");
        job1.setJarByClass(CommonFriends.class);
        job1.setMapperClass(CommonFriendsMapper1.class);
        job1.setReducerClass(CommonFriendsReducer1.class);
        job1.setNumReduceTasks(2);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(IN_PATH));
        FileOutputFormat.setOutputPath(job1, out1);
        boolean isSuccess1 = job1.waitForCompletion(true);
        if (!isSuccess1) {
            return 1;
        }

        // 第二个job
        Path out2 = new Path(OUT_PATH2);
        if (fs.exists(out2)) {
            fs.delete(out2, true);
        }
        Job job2 = Job.getInstance(conf);
        job2.setJobName(TopN.class.getName() + "-2");
        job2.setJarByClass(CommonFriends.class);
        job2.setMapperClass(CommonFriendsMapper2.class);
        job2.setReducerClass(CommonFriendsReducer2.class);
        job2.setNumReduceTasks(1);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job2, out1);
        FileOutputFormat.setOutputPath(job2, out2);
        boolean isSuccess2 = job2.waitForCompletion(true);
        return isSuccess2 ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(HadoopConf.getHAInstance(), new CommonFriends(), args);
        System.exit(result);
    }

    /**
     * 第一阶段的map函数主要完成以下任务
     * 1.遍历原始文件中每行<所有朋友>信息
     * 2.遍历“朋友”集合，以每个“朋友”为键，原来的“人”为值  即输出<朋友,人>
     */
    public static class CommonFriendsMapper1 extends Mapper<LongWritable, Text, Text, Text> {
        private Text K = new Text();
        private Text V = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] personFriends = value.toString().split(":");
            if (personFriends.length != 2) {
                return;
            }
            String[] friends = personFriends[1].split(",");
            for (String friend : friends) {
                K.set(friend);
                V.set(personFriends[0]);
                context.write(K, V);
            }
        }
    }

    /**
     * 第一阶段的reduce函数主要完成以下任务
     * 1.对所有传过来的<朋友，list(人)>进行拼接，输出<朋友,拥有这名朋友的所有人>
     */
    public static class CommonFriendsReducer1 extends Reducer<Text, Text, Text, Text> {
        private StringBuilder sb = new StringBuilder();
        private Text V = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            sb.setLength(0);
            for (Text friend : values) {
                sb.append(friend.toString()).append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            V.set(sb.toString());
            context.write(key, V);
        }
    }

    /**
     * 第二阶段的map函数主要完成以下任务
     * 1.将上一阶段reduce输出的<朋友,拥有这名朋友的所有人>信息中的 “拥有这名朋友的所有人”进行排序 ，以防出现B-C C-B这样的重复
     * 2.将 “拥有这名朋友的所有人”进行两两配对，并将配对后的字符串当做键，“朋友”当做值输出，即输出<人-人，共同朋友>
     */
    public static class CommonFriendsMapper2 extends Mapper<LongWritable, Text, Text, Text> {
        private Text K = new Text();
        private Text V = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] friendPersons = value.toString().split("\t");
            if (friendPersons.length != 2) {
                return;
            }
            String[] persons = friendPersons[1].split(",");
            // 排序很重要
            Arrays.sort(persons);
            // 用户两两配对
            for (int i = 0; i < persons.length - 1; i++) {
                for (int j = i + 1; j < persons.length; j++) {
                    K.set(persons[i] + "-" + persons[j]);
                    V.set(friendPersons[0]);
                    context.write(K, V);
                }
            }
        }
    }

    /**
     * 第二阶段的reduce函数主要完成以下任务
     * 1.<人-人，list(共同朋友)> 中的“共同好友”进行拼接 最后输出<人-人，两人的所有共同好友>
     */
    public static class CommonFriendsReducer2 extends Reducer<Text, Text, Text, Text> {
        private StringBuilder sb = new StringBuilder();
        private Text V = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            sb.setLength(0);
            for (Text friend : values) {
                sb.append(friend.toString()).append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            V.set(sb.toString());
            context.write(key, V);
        }
    }
}