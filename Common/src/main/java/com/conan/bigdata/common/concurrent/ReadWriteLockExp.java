package com.conan.bigdata.common.concurrent;

import java.util.concurrent.TimeUnit;
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
    public static void readWriteLockMethod(Thread thread) {
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        lock.readLock().lock();
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
        lock.readLock().unlock();
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