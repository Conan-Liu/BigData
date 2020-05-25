package com.conan.bigdata.common.concurrent.filedownload;

import java.net.URL;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 用于处理文件下载，多线程下载多个文件，包括大文件小文件
 * 把多个文件处理成多个DownloadTask任务来下载
 * 新建一个文件下载意味着新建一个DownloadFile线程
 */
public class DownloadFile implements Runnable {

    private URL requestURL;
    private long fileSize;
    private StorageFile storageFile;
    private AtomicBoolean taskCanceled = new AtomicBoolean(false);

    public DownloadFile(String filePath) {
        try {
            this.requestURL = new URL(filePath);
            this.fileSize = 70151973L;
            String fileName = filePath.substring(filePath.lastIndexOf("/") + 1);
            storageFile = new StorageFile(this.fileSize, fileName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        if (this.fileSize > 10 * 1024 * 1024) {
            downloadBigFile2(4);
        }
    }

    /**
     * 具体的下载逻辑
     */
    private void downloadFile1() {
        try {
            System.out.println("线程：" + Thread.currentThread().getName() + "，下载文件：" + this.requestURL);
            Thread.sleep(10000000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 对于大文件，一个文件切分成几个小块，多线程下载
     */
    public void downloadBigFile2(int taskCount) {
        long chunkSize = this.fileSize / taskCount;
        long lowerBound = 0;
        long upperBound = 0;
        DownloadTask dt;
        for (int i = taskCount - 1; i >= 0; i--) {
            lowerBound = i * chunkSize;
            if (i == taskCount - 1) {
                upperBound = this.fileSize;
            } else {
                upperBound = lowerBound + chunkSize - 1;
            }

            dt = new DownloadTask(lowerBound, upperBound, this.requestURL, this.storageFile, this.taskCanceled);
            dispatchTask(dt, i);
        }
    }

    private void dispatchTask(DownloadTask dt, int workIndex) {
        Thread workThread = new Thread(dt, "downloader-" + workIndex);
        workThread.start();
    }
}
