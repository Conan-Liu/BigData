package com.conan.bigdata.common.concurrent.filedownload;

/**
 * 多线程下载文件，一个线程负责下载一个文件
 */
public class MultiThreadFileDownload {

    public static void main(String[] args) throws InterruptedException {
        Thread t = new Thread(new DownloadFile("/Users/mw/software/mysql-5.1.38-osx10.5-x86_64.tar.gz"));
        t.start();

    }

}
