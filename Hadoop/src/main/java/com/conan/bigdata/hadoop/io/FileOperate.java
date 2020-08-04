package com.conan.bigdata.hadoop.io;

import com.conan.bigdata.hadoop.util.HadoopConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FileOperate extends Configured implements Tool {

    private static List<FileStatus> listFile = new ArrayList<>();

    private static void showFileStatus(Path path) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://CentOS:8020");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] files = fs.listStatus(path, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                boolean filter = path.getName().endsWith("SUCCESS");
                return !filter;
            }
        });
        for (FileStatus f : files) {
            if (f.isDirectory())
                showFileStatus(f.getPath());
            else if (f.isFile())
                listFile.add(f);
        }
    }

    /**
     * /user/hive/warehouse/dw.db/{assoc_wx_user_track_tmp1,assoc_user_tag_new_tmp1}/  true
     * /user/hive/warehouse/dw.db/assoc_wx_user_track_tmp1   true
     * /user/hive/warehouse/dw.db/assoc_wx_user_track_tmp1/*  false
     */
    private static boolean isExists(FileSystem fileSystem, Path path) throws IOException {
        if (fileSystem.exists(path)) {
            System.out.println("true");
            return true;
        } else {
            System.out.println("false");
            return false;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HadoopConf.getHAInstance();

        int result = ToolRunner.run(conf, new FileOperate(), args);

        System.exit(result);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Path path = new Path(otherArgs[0]);
        FileSystem fileSystem = FileSystem.get(conf);
        isExists(fileSystem, path);
        return 0;
    }
}