package com.conan.bigdata.common.nio;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by Administrator on 2017/6/26.
 */
public class CopyFile {
    private static final String IN_PATH = "E:\\AAA\\aaa.txt";
    private static final String OUT_PATH = "E:\\AAA\\bbb.txt";

    public static void main(String[] args) throws IOException {
        // 获得文件的输入输出流
        FileInputStream fin = new FileInputStream(IN_PATH);
        FileOutputStream fout = new FileOutputStream(OUT_PATH);

        // 获取输入输出通道Channel
        FileChannel fcin = fin.getChannel();
        FileChannel fcout = fout.getChannel();

        // 创建缓冲区
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        while (true) {
            // 重设缓冲区， 使它可以重新接受读入的数据
            buffer.clear();

            //通道数据读到缓冲区，返回字节数，可能为0，如果达到流的末尾，则返回 -1
            int r = fcin.read(buffer);
            if (r == -1) {
                break;
            }

            // 该方法让缓冲区可以将新读入的数据写入另一个通道
            buffer.flip();

            // 将buffer中的数据输出到通道中
            fcout.write(buffer);
        }
    }
}
