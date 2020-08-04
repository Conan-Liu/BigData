package com.conan.bigdata.hbase.mr;

import com.conan.bigdata.hbase.util.HBaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * {@link org.apache.hadoop.hbase.mapreduce.RowCounter} 不能按region来统计记录数
 * 该类实现count(*) group by region的功能
 * 最好的方式是使用计数器，不用输出到hdfs
 * HBase的读取，需要配合Scan
 */

public class RegionRowCounter extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(RegionRowCounter.class);

    // 一个Map对应一个Split，TableInputFormat内部定义一个Split对应一个Region，所以一个Map对应执行一个Region
    public static class RegionRowCounterMapper extends TableMapper<Text, LongWritable> {

        // 这里即使用计数器，又把记录输出到文件中
        private LongWritable regionCnt = new LongWritable(0);
        private String regionNameStr = null;
        private Text regionName = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            InputSplit inputSplit = context.getInputSplit();
            if (inputSplit instanceof TableSplit) {
                regionNameStr = ((TableSplit) inputSplit).getEncodedRegionName();
                regionName.set(regionNameStr);
            }
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

            // 获取表region信息
            long cnt = regionCnt.get() + 1;
            regionCnt.set(cnt);

            context.getCounter("HBase Region", regionNameStr).increment(1);

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(regionName, regionCnt);
        }
    }

    // 选择输出到hdfs，使用一个Reducer来合并Map输出的值，如果没有这个Reducer，会生成和Map任务数一样的文件，这是没必要的
    public static class RegionRowCounterReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            for (LongWritable val : values)
                context.write(key, val);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 1) {
            System.err.println("it is must only one parameter，the table name!!!");
        }
        String tableName = otherArgs[0];

        Scan scan = new Scan();
        // 取消缓存block
        scan.setCacheBlocks(false);
        // 设置hfile文件的timerange，确定扫描的文件数，只有在这个范围内的Cell才能访问到
        scan.setTimeRange(0, Long.MAX_VALUE);
        // 使用过滤器计数行数，不考虑column
        scan.setFilter(new FirstKeyOnlyFilter());

        Job job = Job.getInstance(conf, "RegionRowCounter" + "_" + tableName);
        job.setJarByClass(RegionRowCounter.class);
        job.setNumReduceTasks(0);

        // 这里设置为NullOutputFormat，即不需要输出到文件中，可以直接使用Counter打印在控制台，如果region过多，可以输出到文件
         job.setOutputFormatClass(NullOutputFormat.class);
        // 为了演示，顺带输出到hdfs
        // job.setOutputFormatClass(TextOutputFormat.class);
        // FileOutputFormat.setOutputPath(job, new Path("/user/hive/temp/regionrowcounter/out/"));

        // 直接使用TableMapReduceUtil来配置Mapper操作
        TableMapReduceUtil.initTableMapperJob(tableName, scan, RegionRowCounterMapper.class, Text.class, LongWritable.class, job);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseUtils.getHBaseConf();
        int result = ToolRunner.run(conf, new RegionRowCounter(), args);
        System.exit(result);
    }
}