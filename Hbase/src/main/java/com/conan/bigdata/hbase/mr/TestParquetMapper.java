package com.conan.bigdata.hbase.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import parquet.example.data.Group;

import java.io.IOException;

/**
 * Created by Administrator on 2018/8/28.
 */
public class TestParquetMapper extends Mapper<Void, Group, LongWritable, Text> {

    @Override
    protected void map(Void key, Group value, Context context) throws IOException, InterruptedException {
        String mw_id = value.getString("mw_id", 1);
        long id = value.getLong("id", 0);
        context.write(new LongWritable(id), new Text(mw_id));
    }
}