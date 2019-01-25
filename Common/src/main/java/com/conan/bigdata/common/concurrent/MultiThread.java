package com.conan.bigdata.common.concurrent;

import com.conan.bigdata.common.util.SnowFlakeId;

/**
 * Created by Administrator on 2019/1/24.
 *
 * 以多线程的方式来测试 雪花ID com.conan.bigdata.common.util.SnowFlakeId
 */
public class MultiThread implements Runnable {

    private SnowFlakeId idWorker;

    public MultiThread(int dataCenterId, int workId) {
        idWorker = new SnowFlakeId(dataCenterId, workId);
    }

    @Override
    public void run() {
        for (int i = 0; i < 10; i++) {
            long id = idWorker.nextId();
            System.out.println(Thread.currentThread().getName() + ": " + id + " = " + Long.toBinaryString(id));
        }
    }

    public static void main(String[] args) {
        Thread t1=new Thread(new MultiThread(0,1),"T1");
        Thread t2=new Thread(new MultiThread(0,2),"T2");
        Thread t3=new Thread(new MultiThread(1,1),"T3");
        Thread t4=new Thread(new MultiThread(2,2),"T4");
        t1.start();
        t2.start();
        t3.start();
        t4.start();
    }
}