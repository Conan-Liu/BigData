package com.conan.bigdata.hadoop.mr;

import com.conan.bigdata.hadoop.basic.IntPair;
import com.conan.bigdata.hadoop.util.HadoopConf;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 所有数据排序
 * 1.  利用shuffle过程自动排序， 但是这个只能保证分区内有序， 如果想要全局有序， 只能设置reduce个数为 1
 * 2.  可以自定义一个分区， 按顺序的一个范围把Key送到对应的分区中去， 解释如下
 * 所有 Key < 10000 的数据都发送到Reduce 0；
 * 所有 10000 < Key < 20000 的数据都发送到Reduce 1；
 * 其余的Key都发送到Reduce 2；
 * 这种方法，明显有问题， 如果数据分布很不均匀， 就导致数据倾斜很严重, 所以重点在于如何实现一个 RangePartitioner
 * 能均匀的把数据发送到对应的reducer， 且在范围之间是有序的
 * 参考 Spark sortByKey 方法， 底层就是RangePartitioner实现
 * hadoop 使用 {@link org.apache.hadoop.mapreduce.lib.partition.InputSampler} 来抽样，具体实现参考{@link AllDataSortWithSampler}
 *
 * 下面就是按第 2 步的例子
 */
public class AllDataSort extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(HadoopConf.getHAInstance(), new AllDataSort(), args);
        System.exit(result);
    }

    @Override
    public int run(String[] args) throws Exception {
        return 0;
    }

    public static class AllDataSortMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable> {
        private IntWritable K = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\\s+");
            for (String s : splits) {
                K.set(Integer.parseInt(s));
                context.write(K, NullWritable.get());
            }
        }
    }

    public static class AllDataSortPartitioner extends Partitioner<IntWritable, NullWritable> {
        @Override
        public int getPartition(IntWritable key, NullWritable value, int numPartitions) {
            int k = key.get();
            if (k < 10000) {
                return 0;
            } else if (k < 20000) {
                return 1;
            } else {
                return 2;
            }
        }
    }

    public static class AllDataSortReducer extends Reducer<IntWritable, NullWritable, IntWritable, NullWritable> {

        @Override
        protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            for (NullWritable val : values) {
                context.write(key, NullWritable.get());
            }
        }
    }
}