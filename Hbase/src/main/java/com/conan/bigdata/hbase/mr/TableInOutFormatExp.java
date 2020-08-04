package com.conan.bigdata.hbase.mr;

import com.conan.bigdata.hbase.util.HBaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * HBase读取后重新写入HBase，演示HBase的读写
 * create 'table_test',{NAME => 'f1',BLOOMFILTER => 'NONE'}
 */
public class TableInOutFormatExp extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(TableInOutFormatExp.class);

    /**
     * 从HBase读取，需要使用TableMapReduceUtil.initTableMapperJob，否则报类找不到或认证失败
     */
    public static class TableInOutFormatExpMapper extends TableMapper<Text, Text> {

        private Text K = new Text();
        private Text V = new Text();

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            K.set(Bytes.toString(key.get()) + "_t");
            String city = null;
            List<Cell> list = value.listCells();
            if (list == null || list.size() == 0) {
                city = "0";
            } else {
                // 这里city就一列，就这样简单写下，只为了演示功能
                for (Cell c : value.listCells()) {
                    city = Bytes.toString(c.getFamilyArray(), c.getValueOffset(), c.getValueLength());
                }
            }
            V.set(city);
            LOG.warn("=> {}, {}", K.toString(), V.toString());
            context.write(K, V);
        }
    }

    /**
     * 写入HBase，需要使用TableMapReduceUtil.initTableReducerJob，否则报类找不到或认证失败
     */
    public static class TableInOutFormatExpReducer extends TableReducer<Text, Text, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Put p = new Put(Bytes.toBytes(key.toString()));
            // 只有一列，简单演示，不用追究该逻辑是否合理
            for (Text val : values) {
                p.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("c1"), Bytes.toBytes(val.toString()));
            }
            context.write(NullWritable.get(), p);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "TableInOutFormatExp");
        job.setJarByClass(TableInOutFormatExp.class);

        String tableName = "wx_user_tag";
        Scan scan = new Scan();
        scan.setCacheBlocks(true);
        scan.setBatch(1000);
        scan.setTimeRange(0, Long.MAX_VALUE);
        scan.setMaxVersions(1);

        // 推荐使用该工具类来处理
        TableMapReduceUtil.initTableMapperJob(tableName, scan, TableInOutFormatExpMapper.class, Text.class, Text.class, job, true, true, TableInputFormat.class);
        TableMapReduceUtil.initTableReducerJob(tableName, TableInOutFormatExpReducer.class, job);

        // 不加这个，总是会包HBase的相关类找不到，可以指直接调用TableMapReduceUtil初始化方法
        // TableMapReduceUtil.addDependencyJars(job);
        // TableMapReduceUtil.initCredentials(job);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseUtils.getHBaseConf();
        int result = ToolRunner.run(conf, new TableInOutFormatExp(), args);
        System.exit(result);
    }
}