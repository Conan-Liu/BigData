package com.conan.bigdata.hbase.mr;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.UUID;

public class LoadDataToHbaseMapper extends Mapper<LongWritable,Text,ImmutableBytesWritable,Put>{
    //map
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] splits = line.split("\\|");
            String rowKey = createRowKey(splits);

            ImmutableBytesWritable hkey = new ImmutableBytesWritable(Bytes.toBytes(rowKey));

            // 可以使用三种输出的Value类型， Put KeyValue Text，推荐KeyValue
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("c1"),Bytes.toBytes(line));
            // KeyValue kv=new KeyValue(Bytes.toBytes(rowKey),Bytes.toBytes("f1"),Bytes.toBytes("c1"),Bytes.toBytes(line));
            context.write(hkey,put);
    }

    //create rowkey
    public static String createRowKey(String[] splits) {
        String serv_number = reverse(splits[2]);
        String statis_day = splits[0];
        //uuid加密(MD5)，密文是数字和小写字母，取前三位后三位;
        String randomStr = UUID.randomUUID().toString();
        String MD5Str = toMD5(randomStr);
        String rowKey = serv_number + statis_day + MD5Str;
        return rowKey;
    }

    //reverse
    public static String reverse(String s) {

        return new StringBuffer(s).reverse().toString();
    }

    //toMD5
    public static String toMD5(String plainText) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(plainText.getBytes());
            byte[] b = md.digest();
            int i;
            StringBuffer buf = new StringBuffer("");
            for (int offset = 0; offset < b.length; offset++) {
                i = b[offset];
                if (i < 0)
                    i += 256;
                if (i < 16)
                    buf.append("0");
                buf.append(Integer.toHexString(i));
            }
            return buf.toString().substring(0, 6) + buf.toString().substring(26, 32);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        String randomStr = UUID.randomUUID().toString();
        String MD5Str = toMD5(randomStr);
        System.out.println(MD5Str);
    }

}
