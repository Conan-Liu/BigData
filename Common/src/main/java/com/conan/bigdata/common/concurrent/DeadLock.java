package com.conan.bigdata.common.concurrent;

import com.conan.bigdata.common.util.Tools;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 死锁演示
 * <p>
 * 死锁场景：t1线程持有A资源，t2线程持有B资源，且两个线程一直占有资源，不支持抢占
 * 那么，如果t1接下来要申请B资源，t2要申请A资源，就会发生死锁
 */
public class DeadLock {

    private static class A implements Runnable {

        private Object objA;
        private Object objB;

        public A(Object objA, Object objB) {
            this.objA = objA;
            this.objB = objB;
        }

        @Override
        public void run() {
            synchronized (objA) {
                System.out.println("访问到objA");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                synchronized (objB) {
                    System.out.println("访问到objB");
                }
            }
        }
    }

    private static class B implements Runnable {

        private Object objA;
        private Object objB;

        public B(Object objA, Object objB) {
            this.objA = objA;
            this.objB = objB;
        }

        @Override
        public void run() {
            synchronized (objB) {
                System.out.println("访问到objB");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                synchronized (objA) {
                    System.out.println("访问到objA");
                }
            }
        }
    }

    // 声明两个资源
    private static final String RESOURCE_A = "A";
    private static final String RESOURCE_B = "B";

    public static void main(String[] args) {
        // objectMethod();
        // synchronizedMethod();
        lockMethod();
    }

    /**
     * 定义了两个对象，作为参数传递给两个线程，然后启动
     */
    private static void objectMethod() {
        Object objA = new Object();
        Object objB = new Object();
        Thread tt1 = new Thread(new A(objA, objB), "tt1");
        Thread tt2 = new Thread(new B(objA, objB), "tt2");
        tt1.start();
        tt2.start();
    }

    /**
     * 通过内部锁来演示死锁，静态变量作为参数被两个线程共享
     */
    private static void synchronizedMethod() {
        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                synchronized (RESOURCE_A) {
                    System.out.println("t1 get resource A");
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    synchronized (RESOURCE_B) {
                        System.out.println("t1 get resource B");
                    }
                }
            }
        }, "synchronized-t1");

        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                synchronized (RESOURCE_B) {
                    System.out.println("t1 get resource B");
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    synchronized (RESOURCE_A) {
                        System.out.println("t1 get resource A");
                    }
                }
            }
        }, "synchronized-t2");

        t1.start();
        t2.start();
    }

    /**
     * 显示锁演示死锁，Lock锁可以不用指定资源加锁，直接对执行的代码加锁
     */
    private static final Lock LOCK_1 = new ReentrantLock();
    private static final Lock LOCK_2 = new ReentrantLock();

    private static void lockMethod() {
        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                LOCK_1.lock();
                try {
                    Tools.randomSleep(1000, false);
                    System.out.println(Thread.currentThread().getName() + "已获取 lock1 锁");
                    LOCK_2.lock();
                    try {
                        System.out.println(Thread.currentThread().getName() + "已获取 lock2 锁");
                    } finally {
                        LOCK_2.unlock();
                    }
                } finally {
                    LOCK_1.unlock();
                }

            }
        }, "lock-t1");

        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                LOCK_2.lock();
                try {
                    Tools.randomSleep(1000, false);
                    System.out.println(Thread.currentThread().getName() + "已获取 lock2 锁");
                    LOCK_1.lock();
                    try {
                        System.out.println(Thread.currentThread().getName() + "已获取 lock1 锁");
                    } finally {
                        LOCK_1.unlock();
                    }
                } finally {
                    LOCK_2.unlock();
                }

            }
        }, "lock-t2");

        t1.start();
        t2.start();
    }
}