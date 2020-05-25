package com.conan.bigdata.common.concurrent.filedownload;

import com.conan.bigdata.common.util.Tools;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 下载任务类，用来处理每个下载任务
 * <p>
 * 考虑到大文件可以分成几块一起下载，那么一个大文件可以对应多个DownloadTask
 * 一个小文件对应一个DownloadTask
 * 多线程下载文件块
 */
public class DownloadTask implements Runnable {

    private long lowerBound;
    private long upperBound;
    private DownloadBuffer xbuf;
    private URL requestURL;
    private AtomicBoolean cancelFlag;

    public DownloadTask(long lowerBound, long upperBound, URL requestURL, StorageFile storageFile, AtomicBoolean cancelFlag) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.requestURL = requestURL;
        this.xbuf = new DownloadBuffer(lowerBound, upperBound, storageFile);
        this.cancelFlag = cancelFlag;
    }

    // 发起分块下载的请求
    private static InputStream issueRequest(URL requestURL, long lowerBound, long upperBound) {
        // code
        return null;
    }

    @Override
    public void run() {
        ReadableByteChannel channel = null;
        try {
            channel = Channels.newChannel(issueRequest(this.requestURL, this.lowerBound, this.upperBound));
            ByteBuffer buf = ByteBuffer.allocate(1024 * 1024);
            while (!this.cancelFlag.get() && channel.read(buf) > 0) {
                this.xbuf.write(buf);
                buf.clear();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            Tools.closeChannel(channel);
        }
    }
}
