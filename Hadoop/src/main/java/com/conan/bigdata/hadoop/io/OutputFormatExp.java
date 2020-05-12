package com.conan.bigdata.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * 对于文件的输出，可以直接继承自FileOutputFormat
 * <p>
 * 演示输出不同的文件，类似MultipleOutput
 */
public class OutputFormatExp<K, V> extends FileOutputFormat<K, V> {

    public static String SEPERATOR = "keyvalue.input.separator";

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {

        Configuration conf = job.getConfiguration();
        boolean isCompressed = getCompressOutput(job);
        String keyValueSeparator = conf.get(SEPERATOR, ":");
        CompressionCodec codec = null;
        String extension = "";
        if (isCompressed) {
            // 从配置的参数里面，反射加载压缩的类，用于数据压缩
            Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
            codec = ReflectionUtils.newInstance(codecClass, conf);
            extension = codec.getDefaultExtension();
        }

        Path file = getDefaultWorkFile(job, extension);
        FileSystem fs = file.getFileSystem(conf);
        // 根据是否压缩，选择不同的输出流
        if (!isCompressed) {
            FSDataOutputStream fileOut = fs.create(file, false);
            return null;
        } else {
            FSDataOutputStream fileOut = fs.create(file, false);
            return null;
        }
    }

    private static class RecordWriterExp<K, V> extends RecordWriter<K, V> {
        // 需要定义一个输出流来处理数据
        private FSDataOutputStream fsout;

        @Override
        public void write(K key, V value) throws IOException, InterruptedException {

        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {

        }
    }
}
