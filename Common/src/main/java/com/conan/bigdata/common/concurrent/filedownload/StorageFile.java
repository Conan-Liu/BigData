package com.conan.bigdata.common.concurrent.filedownload;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 文件存储类
 */
public class StorageFile implements Closeable {

    // 如果是final修饰，构造函数中不能用try-catch，这是为什么？
    private RandomAccessFile storeFile;
    private FileChannel storeChannel;
    protected AtomicLong totalWrite;

    public StorageFile(long fileSize, String fileName) {
        try {
            String fullFileName = "";
            String localFileName = "";

            storeFile = new RandomAccessFile(localFileName, "rw");
            storeChannel = storeFile.getChannel();
            totalWrite = new AtomicLong(0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String createStoreFile(long fileSize,String fullName) throws IOException{
        File file=new File(fullName);
        RandomAccessFile raf=new RandomAccessFile(file,"rw");
        raf.setLength(fileSize);
        return fullName;
    }

    public int store(long offset, ByteBuffer byteBuffer) throws IOException {
        int length = byteBuffer.limit();
        storeChannel.write(byteBuffer, offset);
        totalWrite.addAndGet(length);
        return length;
    }

    public long getTotalWrite() {
        return totalWrite.get();
    }

    @Override
    public void close() throws IOException {
        if(storeChannel.isOpen()){
            storeChannel.close();
        }
    }
}
