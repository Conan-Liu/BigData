package com.conan.bigdata.hadoop.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.*;

public class JavaAPI {

    public FileSystem getFileSystem() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        return fileSystem;
    }

    /**
     * IOUtils 工具处理流
     * hdfs 读写文件，就是对输入流输出流的操作
     */
    @Test
    public void upload() throws IOException {
        FileSystem fileSystem = getFileSystem();
        FileInputStream fin = new FileInputStream("/Users/mw/bigdata/hadoop/hadoop-2.6.0-cdh5.10.0/aa");
        FSDataOutputStream fout = fileSystem.create(new Path("/user/mw/test/in/bb"), true);

        IOUtils.copyBytes(fin, fout, 40960, true);
    }

    // 两个小文件合并成hdfs上的大文件，以此类推
    @Test
    public void mergeUpload() throws IOException {
        FileSystem fileSystem = getFileSystem();
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/user/mw/test/in/merge"), true);

        FileInputStream fin1 = new FileInputStream("/Users/mw/bigdata/hadoop/hadoop-2.6.0-cdh5.10.0/bb");
        FileInputStream fin2 = new FileInputStream("/Users/mw/bigdata/hadoop/hadoop-2.6.0-cdh5.10.0/cc");

        // 注意着里不能为true，不能关闭output的输出管道，等数据读完了才能关闭
        IOUtils.copyBytes(fin1, fsDataOutputStream, 4096, false);
        fin1.close();
        IOUtils.copyBytes(fin2, fsDataOutputStream, 4096, true);
    }

    @Test
    public void download() throws IOException {
        FileSystem fileSystem = getFileSystem();
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/user/mw/test/in/merge"), 4096);
        // 这里为什么有空
        byte[] b = new byte[4096];
        String s;
        int byteLength;
        while ((byteLength=fsDataInputStream.read(b)) >0) {
            // b.length 是数组的长度，为4096
            s = new String(b,0,byteLength-1);
            System.out.println(s);
        }

        System.out.println("***********************************");
        fsDataInputStream.seek(0);
        BufferedReader br=new BufferedReader(new InputStreamReader(fsDataInputStream));
        String line;
        while ((line=br.readLine())!=null){
            System.out.println(line);
        }

        System.out.println("***********************************");
        // 简单操作，直接输出控制台，前面文件流指针已经是到末尾，需要重定向一下
        fsDataInputStream.seek(0);
        IOUtils.copyBytes(fsDataInputStream, System.out, 4096, true);
    }

    @Test
    public void copyFile() throws Exception {
        FileSystem fs = getFileSystem();
        fs.copyFromLocalFile(true, true, new Path(""), new Path(""));
        fs.copyToLocalFile(new Path(""), new Path(""));
    }
}
