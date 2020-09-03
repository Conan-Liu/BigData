package com.conan.bigdata.hadoop.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;

import java.io.IOException;

/**
 *
 */
public class FileSystemExp {

    public static void main(String[] args) throws IOException {
        FileSystem fs=FileSystem.get(new Configuration());

        FileSystem fs1=new RawLocalFileSystem();

        FileSystem fs2=new LocalFileSystem();
    }
}
