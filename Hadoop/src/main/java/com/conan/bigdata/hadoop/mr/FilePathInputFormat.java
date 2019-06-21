package com.conan.bigdata.hadoop.mr;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

/**
 * Created by Administrator on 2019/6/21.
 */
class FilePathRecordReader extends RecordReader<Text, Text> {

    String fileName = null;

    LineRecordReader lineRecordReader = new LineRecordReader();
    private Text K = new Text();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        lineRecordReader.initialize(split, context);
        fileName = ((FileSplit) split).getPath().getName();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return lineRecordReader.nextKeyValue();
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        K.set(fileName);
        return K;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return lineRecordReader.getCurrentValue();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        lineRecordReader.close();
    }
}

/**
 * 这个输入类， 输入的数据格式 <K, V> 是 <Text, Text>
 * 这个K 是输入的文件名
 */

public class FilePathInputFormat extends FileInputFormat<Text, Text> {

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FilePathRecordReader filePathRecordReader = new FilePathRecordReader();
        filePathRecordReader.initialize(split, context);
        return filePathRecordReader;
    }
}