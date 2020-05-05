package com.conan.bigdata.hadoop.io;

import com.google.common.base.Charsets;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;
import java.util.Arrays;

/**
 * 自定义InputFormat，两种方式
 * 1.可以继承自InputFormat，但是需要实现getSplits和createRecordReader两个方法
 * 2.可以继承自FileInputFormat，只需要实现createRecordReader方法
 *
 * 例子：实现逗号分割的K，V
 * 当然普通的TextInputFormat也可以实现，这里只为了演示
 */
public class InputFormatExp extends FileInputFormat<IntWritable,IntWritable> {

    /**
     * 判断压缩格式是否支持split
     * FileInputFormat中isSplitable默认是true，也就是说他的子类如果不重写该方法，默认都是可以分割的
     * 如果在特殊情况下，想要不分割文件，可以自定义一个FileInputFormat的子类，重写该方法返回false即可
     */
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        final CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(filename);
        if (null == codec) {
            return true;
        }
        // 目前hadoop集成的支持分割的就只有 Bzip2
        return codec instanceof SplittableCompressionCodec;
    }

    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        String delimiter = context.getConfiguration().get(
                "textinputformat.record.delimiter");
        byte[] recordDelimiterBytes = null;
        if (null != delimiter)
            recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
        return new RecordReaderExp();
    }


    public static class RecordReaderExp extends RecordReader<IntWritable, IntWritable> {

        public static final String KEYVALUE_SEPERATOR = "keyvalue.separator";
        // 对于一般的文件操作，重用LineRecordReader即可
        private LineRecordReader reader;
        private String separator;
        private IntWritable K;
        private IntWritable V;
        private String[] inputValue;

        public RecordReaderExp() {
            reader = new LineRecordReader();
            K=new IntWritable();
            V=new IntWritable();
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            separator=context.getConfiguration().get(KEYVALUE_SEPERATOR,",");
            reader.initialize(split, context);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (reader.nextKeyValue()) {
                inputValue = reader.getCurrentValue().toString().split(separator);
                System.out.println(Arrays.toString(inputValue));
                if (inputValue.length == 2) {
                    // 这里可能报NumberFormatException，所以处理这个时候可以考虑引入try-catch
                    K.set(Integer.parseInt(inputValue[0]));
                    V.set(Integer.parseInt(inputValue[1]));
                } else {
                    K.set(0);
                    V.set(0);
                }
                return true;
            } else {
                return false;
            }
        }

        @Override
        public IntWritable getCurrentKey() throws IOException, InterruptedException {
            return this.K;
        }

        @Override
        public IntWritable getCurrentValue() throws IOException, InterruptedException {
            return this.V;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return reader.getProgress();
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

}
