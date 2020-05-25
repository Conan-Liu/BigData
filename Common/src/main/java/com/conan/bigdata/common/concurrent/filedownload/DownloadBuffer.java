package com.conan.bigdata.common.concurrent.filedownload;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * 文件下载缓冲区
 */
public class DownloadBuffer implements Closeable {

    private long globalOffset;
    private long upperBound;
    private int offset = 0;
    public final ByteBuffer byteBuffer;
    private final StorageFile storageFile;

    public DownloadBuffer(long globalOffset, long upperBound, final StorageFile storageFile) {
        this.globalOffset = globalOffset;
        this.upperBound = upperBound;
        this.byteBuffer = ByteBuffer.allocate(1024 * 1024);
        this.storageFile = storageFile;
    }

    public void write(ByteBuffer buffer) throws IOException {
        int length = buffer.position();
        int capactiy = this.byteBuffer.capacity();

        if ((this.offset + length) > capactiy || capactiy == length) {
            flush();
        }

        this.byteBuffer.position(this.offset);
        buffer.flip();
        this.byteBuffer.put(buffer);
        this.offset += length;
    }

    public void flush() throws IOException {
        this.byteBuffer.flip();
        int length = this.storageFile.store(this.globalOffset, this.byteBuffer);
        this.byteBuffer.clear();
        this.globalOffset += length;
        this.offset = 0;
    }

    @Override
    public void close() throws IOException {
        if(this.globalOffset<this.upperBound){
            flush();
        }
    }
}
