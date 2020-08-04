package com.conan.bigdata.hbase.mr;

import com.conan.bigdata.hbase.util.HBaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 该hbase表只存储两个表的数据
 * dw.assoc_user_tag_new   大一号
 * dw.assoc_wx_user_track  大二号
 * <p>
 * create 'wx_user_tag',{NAME => 'f1',COMPRESSION => 'SNAPPY',BLOOMFILTER => 'NONE'},SPLITS => ['5']
 */
public class WxUserTagToHbase extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(WxUserTagToHbase.class);

    // 大一号文件路径
    private static final String DAYI_PATH = "/user/hive/warehouse/ods.db/assoc_user_tag_new/";
    // 大二号文件路径
    private static final String DAER_PATH = "/user/hive/warehouse/ods.db/assoc_wx_user_track/";
    private static final Path OUTPUT_PATH = new Path("/user/hive/temp/hbase/wx_user_tag");

    public static class WxTagToHbaseMapper extends Mapper<Void, Group, ImmutableBytesWritable, KeyValue> {

        private ImmutableBytesWritable K = new ImmutableBytesWritable();
        private KeyValue V = null;
        private boolean isDayi = true;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            InputSplit inputSplit = context.getInputSplit();
            if (inputSplit instanceof FileSplit) {
                FileSplit split = (FileSplit) inputSplit;
                String filePath = split.getPath().toString();
                isDayi = filePath.contains("assoc_user_tag_new");
            } else {
                throw new IOException("this split is not FileSplit ...");
            }
        }

        @Override
        protected void map(Void key, Group value, Context context) throws IOException, InterruptedException {
            String app_id = isDayi ? "dayi" : value.getString("app_id", 0);
            long user_id = value.getLong("user_id", 0);
            String city = value.getString("city", 0);
            String rowKey = null;
            // 生成row_key    后缀 _1 表示大一号   后缀 _2 表示大二号
            // 目前大一未上线
            if ("dayi".equals(app_id)) {
                // rowKey = HbaseUtils.lpad(String.valueOf(user_id), 1);
                rowKey = "1";
            } else if ("wx8624eb15102147c6".equals(app_id)) {
                // rowKey = HbaseUtils.lpad(String.valueOf(user_id), 2);
                rowKey = "2";
            }
            if (rowKey != null) {
                K.set(Bytes.toBytes(rowKey));
                V = new KeyValue(Bytes.toBytes(rowKey), Bytes.toBytes("f1"), Bytes.toBytes("c1"), Bytes.toBytes(city));
                context.write(K, V);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(OUTPUT_PATH))
            fs.delete(OUTPUT_PATH, true);


        Job job = Job.getInstance(conf, "WxUserTagToHbase");
        job.setJarByClass(WxUserTagToHbase.class);
        job.setMapperClass(WxTagToHbaseMapper.class);
        job.setInputFormatClass(ParquetInputFormat.class);
        ParquetInputFormat.setReadSupportClass(job, GroupReadSupport.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);

        FileInputFormat.setInputPaths(job, new Path(DAYI_PATH), new Path(DAER_PATH));
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        Connection connection = HBaseUtils.getConnection();
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf("wx_user_tag");
        Table table = connection.getTable(tableName);

        HFileOutputFormat2.configureIncrementalLoad(job, table.getTableDescriptor(), connection.getRegionLocator(tableName));

        boolean isSuccess = job.waitForCompletion(true);
        if (isSuccess) {
            LoadIncrementalHFiles loadHFile = new LoadIncrementalHFiles(conf);
            loadHFile.doBulkLoad(OUTPUT_PATH, admin, table, connection.getRegionLocator(tableName));
            System.out.println("Bulk load completed ......");
            return 0;
        } else {
            System.out.println("Job error ......");
            return 1;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseUtils.getHBaseConf();
        int result = ToolRunner.run(conf, new WxUserTagToHbase(), args);
        System.exit(result);
    }
}