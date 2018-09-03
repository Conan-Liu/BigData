package com.conan.bigdata.hbase.api;

import com.conan.bigdata.hbase.util.HBaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Map;

/**
 * Created by Administrator on 2018/9/3.
 */
public class GetConfiguration extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        for (Map.Entry<String, String> c : conf) {
            System.out.println(c.getKey() + " = " + c.getValue());
        }
        return 0;
    }

    public static void main(String[] args) {
        try {
            int result = ToolRunner.run(HBaseUtils.getHBaseConf(), new GetConfiguration(), args);
            System.exit(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}