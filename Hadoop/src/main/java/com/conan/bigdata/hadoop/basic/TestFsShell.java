package com.conan.bigdata.hadoop.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by Administrator on 2017/3/25.
 */
public class TestFsShell extends Configured implements Tool {
    public static void main(String[] args) {
        try {
            int result = ToolRunner.run(new Configuration(), new TestFsShell(), args);
            System.out.println("result = " + result);
        } catch (Exception e) {
            System.out.println("出错啦");
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        System.out.println(conf.get("fs.defaultFS"));
        FsShell shell = new FsShell(conf);
        Path trashDir = shell.getCurrentTrashDir();
        System.out.println("trashDir : " + trashDir.getName());
        try {
            shell.run(new String[]{"-ls", "/user"});    // 这条命令相当于 hdfs dfs -ls /user
            return 0;
        } catch (Exception e) {
            return 1;
        }
    }
}
