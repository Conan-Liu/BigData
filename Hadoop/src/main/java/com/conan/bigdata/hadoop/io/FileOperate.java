package com.conan.bigdata.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Conan on 2019/5/11.
 */
public class FileOperate {

    private static List<FileStatus> listFile = new ArrayList<>();

    public static void showFileStatus(Path path) throws IOException {
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

    public static void main(String[] args) throws IOException {
        showFileStatus(new Path("/user/hadoop/output/temp"));
        Arrays.sort(listFile.toArray());
        for (FileStatus f : listFile) {
            System.out.println(f.getPath().toString());
        }
    }
}