package com.conan.bigdata.hive.fileformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * 注意hive目前是基于{@link org.apache.hadoop.mapred.InputFormat}老版的
 * <p>
 * 这里实现一个简单的例子
 * 文本文件是以其它字符分割，处理成hive可以用 ｜ 来读取
 * <p>
 * 创建hive表的时候只需指定
 * row format delimited fields terminated by '|'
 * stored as inputformat 'com.conan.bigdata.hive.fileformat.HiveInputFormatExp'
 */
public class HiveInputFormatExp extends TextInputFormat {

    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter) throws IOException {
        reporter.setStatus(genericSplit.toString());

        // 第一种
        // return new HiveRecordReader1(job, (FileSplit) genericSplit);

        // 第二种
        return new HiveRecordReader2(job, (FileSplit) genericSplit);
    }

    private static class HiveRecordReader1 implements RecordReader<LongWritable, Text> {

        private LineRecordReader reader;

        public HiveRecordReader1(Configuration job, FileSplit split) throws IOException {
            reader = new LineRecordReader(job, split);
        }

        /**
         * 这里使用 LineRecordReader，需要注意调用顺序
         * 先使用LineRecordReader对象给K，V填充上数据，然后再对V进行字符串替换
         * 最后该自定义的RecordReader返回K，V给hive框架使用
         *
         * 这个顺序不能错，如过像注释一样，顺序颠倒，那么达不到想要的效果
         */
        @Override
        public boolean next(LongWritable key, Text value) throws IOException {
            if(reader.next(key,value)) {
                String ss = value.toString().replaceAll(",", "\\|");
                value.set(ss);
                // 可以在控制台打印出结果，作为调试
                System.out.println(value);
                return true;
            }else{
                return false;
            }

            // 这里顺序不对，没起到替换的效果
            // String ss = value.toString().replaceAll(",", "\\|");
            // value.set(ss);
            // reader.next(key,value);
        }

        @Override
        public LongWritable createKey() {
            return reader.createKey();
        }

        @Override
        public Text createValue() {
            return reader.createValue();
        }

        @Override
        public long getPos() throws IOException {
            return reader.getPos();
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

        @Override
        public float getProgress() throws IOException {
            return reader.getProgress();
        }
    }

    private static class HiveRecordReader2 extends LineRecordReader {

        public HiveRecordReader2(Configuration job, FileSplit split) throws IOException {
            super(job, split);
        }

        @Override
        public synchronized boolean next(LongWritable key, Text value) throws IOException {
            if(super.next(key,value)) {
                String ss = value.toString().replaceAll(",", "\\|");
                value.set(ss);
                return true;
            }else{
                return false;
            }
        }
    }

    public static void main(String[] args) {
        String a = "1,name,gender".replaceAll(",", "\\|");
        System.out.println(a);
    }
}
