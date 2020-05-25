package com.conan.bigdata.common.concurrent.thread;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RaceConditionDemo {

    /**
     * 定义一个ID生成器
     */
    private static class IDGenerator {

        private final static IDGenerator INSTANCE = new IDGenerator();
        private final static int SEQ_UPPER_LIMIT = 999;
        // 共享变量，线程共有，可能存在线程安全问题
        private int sequence = 0;
        private DecimalFormat df = new DecimalFormat("000");
        private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        // 锁句柄不允许修改，用final修饰
        private final Object objLock=new Object();
        private final Lock relock=new ReentrantLock();

        private IDGenerator() {

        }

        // 对象锁
        private int nextSequence1() {
            synchronized (objLock) {
                if (sequence >= SEQ_UPPER_LIMIT) {
                    sequence = 0;
                } else {
                    sequence++;
                }
                return sequence;
            }
        }

        // 方法锁
        private synchronized int nextSequence2() {
                if (sequence >= SEQ_UPPER_LIMIT) {
                    sequence = 0;
                } else {
                    sequence++;
                }
                return sequence;
        }

        // 可重入锁Lock
        private int nextSequence3() {
            relock.lock();
            try {
                if (sequence >= SEQ_UPPER_LIMIT) {
                    sequence = 0;
                } else {
                    sequence++;
                }
                return sequence;
            }finally{
                // 释放锁，写在finally中，避免抛异常后造成锁泄漏
                relock.unlock();
            }
        }

        // 带上时间，逻辑上保持唯一
        public String nextID() {
            // 线程私有
            String timestamp = sdf.format(new Date());
            String nextSeq = df.format(nextSequence3());
            return timestamp + "-" + nextSeq;
        }

        public static IDGenerator getInstance() {
            return INSTANCE;
        }
    }

    private static class WorkThread implements Runnable {

        private final int requestCount;

        public WorkThread(int requestCount) {
            this.requestCount = requestCount;
        }

        @Override
        public void run() {
            int i = requestCount;
            String requestID;
            IDGenerator idGenerator = IDGenerator.getInstance();
            while (i-- > 0) {
                requestID = idGenerator.nextID();
                System.out.println(Thread.currentThread().getName() + " = " + requestID);
            }
        }
    }

    public static void main(String[] args) {
        int numberOfThreads = Runtime.getRuntime().availableProcessors();
        Thread[] workThreads = new Thread[numberOfThreads];
        WorkThread tt=new WorkThread(10);
        for(int i=0;i<numberOfThreads;i++){
            workThreads[i]=new Thread(tt,"work-"+i);
        }

        for(Thread t:workThreads){
            t.start();
        }
    }
}
