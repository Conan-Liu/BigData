package com.conan.bigdata.common.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Synchronized 锁是原子性，任何操作都是互斥， 效率相对低
 * ReadWriteLock  实现了线程之间的读共享，其它有写操作互斥，相对高效
 */
public class ReadWriteLockExp {

    // synchronized 可以看出，这两个线程是顺序执行的，一个获取了锁，一个只能暂停等待
    public synchronized static void syncMethod(Thread thread) {
        long start = System.nanoTime();
        System.out.println("start time:" + start);
        for (int i = 0; i < 5; i++) {
            try {
                TimeUnit.SECONDS.sleep(1);
                System.out.println(thread.getName() + " : 正在进行读操作......");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println(thread.getName() + " : 读操作完毕!");
        long end = System.nanoTime();
        System.out.println("end time:" + end);
        System.out.println("used time:" + (end - start));
    }

    // ReetrantReadWriteLock 可重入的读写锁,可重入锁，就是说一个线程在获取某个锁后，还可以继续获取该锁，即允许一个线程多次获取同一个锁
    // 两个线程可以一起读， 效率高
    // 实现机制复杂，适用场景读操作多，
    public static void readWriteLockMethod(Thread thread) {
        ReadWriteLock lock = new ReentrantReadWriteLock();
        lock.readLock().lock();
        try {
            long start = System.nanoTime();
            System.out.println("start time:" + start);
            for (int i = 0; i < 5; i++) {
                try {
                    TimeUnit.SECONDS.sleep(1);
                    System.out.println(thread.getName() + " : 正在进行读操作......");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            long end = System.nanoTime();
            System.out.println("end time:" + end);
            System.out.println("used time:" + (end - start));
        } finally {
            // readLock 返回显示锁，写在finally块中防止锁泄漏
            lock.readLock().unlock();
        }
    }

    /**
     * volatile 的可见性，配合synchronized的原子性，实现一个简易版的读写锁
     */
    public static class Counter {
        private volatile long count = 0;

        public long getCount() {
            return this.count;
        }

        public void increment() {
            synchronized (this) {
                count++;
            }
        }
    }

    public static void main(String[] args) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                syncMethod(Thread.currentThread());
//                readWriteLockMethod(Thread.currentThread());
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                syncMethod(Thread.currentThread());
//                readWriteLockMethod(Thread.currentThread());
            }
        }).start();
    }
}