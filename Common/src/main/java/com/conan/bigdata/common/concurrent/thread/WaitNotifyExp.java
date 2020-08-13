package com.conan.bigdata.common.concurrent.thread;

import com.conan.bigdata.common.util.Tools;

import java.util.Date;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 多线程环境下wait和notify的演示
 * 一。超时机制演示，两种方式实现
 * 1. 需要使用 wait(long timeout)方法
 * 2. 使用Condition 条件变量
 */
public class WaitNotifyExp {

    // 假定的一个对象
    private static final Object OBJ = new Object();
    private static final Lock LOCK = new ReentrantLock();
    private static final Condition condition = LOCK.newCondition();
    private static final CountDownLatch LATCH = new CountDownLatch(4);
    private static boolean isReady = false;
    private static final Random random = new Random();

    public static void main(String[] args) throws InterruptedException {

        // objectWait();
        // conditionWait1();
        // conditionWait2();
        countDownLatch();
    }

    /**
     * 该方法是Object.wait演示超时
     */
    private static void objectWait() throws InterruptedException {
        final Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    synchronized (OBJ) {
                        while (true) {
                            System.out.println("get lock...1");
                            isReady = random.nextInt(100) > 100 ? true : false;
                            if (isReady) {
                                OBJ.notify();
                                OBJ.wait();
                            } else {
                                OBJ.wait();
                            }
                            Tools.randomSleep(400, false);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, "t");
        // 以守护线程的形式运行，无需wait
        t.setDaemon(true);
        t.start();

        // 主线程
        objectWaiter(1000);
    }

    private static void objectWaiter(long timeOut) throws InterruptedException {
        if (timeOut < 0) {
            throw new IllegalArgumentException("超时时长不可以为负数");
        }

        long start = System.currentTimeMillis();
        long waitTime;
        long now;
        synchronized (OBJ) {
            while (!isReady) {
                System.out.println("get lock...2");
                now = System.currentTimeMillis();
                waitTime = timeOut - (now - start);
                System.out.println("remain " + waitTime);
                if (waitTime <= 0) {
                    break;
                }
                OBJ.notify();
                // 允许一定的超时时间，如果时间到了，没有其它线程唤醒，JVM会自动唤醒
                OBJ.wait(waitTime);
            }

            if (isReady) {
                // 执行目标动作
                System.out.println("这是执行的动作...");
            } else {
                System.out.println("等待超时，退出...");
            }
        }
    }


    /**
     * Condition 基本操作，需要配合显示锁
     */
    private static void conditionWait1() {
        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        LOCK.lock();
                        System.out.println("Thread - 1");
                        condition.signal();
                        condition.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } finally {
                        LOCK.unlock();
                    }
                    Tools.randomSleep(1000, false);
                }
            }
        }, "t1");
        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        LOCK.lock();
                        System.out.println("Thread - 2");
                        condition.signal();
                        condition.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } finally {
                        LOCK.unlock();
                    }
                    Tools.randomSleep(1000, false);
                }
            }
        }, "t2");

        t1.start();
        t2.start();

    }

    /**
     * 该方法是Condition条件变量演示超时
     */
    private static void conditionWait2() {

        final Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    LOCK.lock();
                    try {
                        System.out.println("lock - 1");
                        isReady = random.nextInt(100) < 25 ? true : false;
                        if (isReady) {
                            condition.signal();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        LOCK.unlock();
                    }
                    Tools.randomSleep(500, false);
                }
            }
        }, "t");

        t.setDaemon(true);
        t.start();

        conditionWaiter2(2000);
    }

    private static void conditionWaiter2(long timeOut) {
        if (timeOut < 0) {
            throw new IllegalArgumentException("超时时长不可以为负数");
        }
        final Date deadline = new Date(System.currentTimeMillis() + timeOut);
        boolean continueToWait = true;
        LOCK.lock();
        try {
            System.out.println("lock - 2");
            while (!isReady) {
                if (!continueToWait) {
                    break;
                }
                // deadline 表示最后的超时时间，如果返回true则表示还未超时，是由其它线程执行signal和signalAll唤醒的
                continueToWait = condition.awaitUntil(deadline);
            }

            if (continueToWait) {
                System.out.println("未超时，可以执行一些操作...");
            } else {
                System.out.println("超时Timeout");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            LOCK.unlock();
        }
    }

    /**
     * 倒计时协调器
     * {@link java.util.concurrent.CountDownLatch} 演示
     * 只能提供一次计数
     */
    private static int data;

    private static void countDownLatch() throws InterruptedException {
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 1; i < 3; i++) {
                    data = i;
                    /**
                     * new的时候传入计数值4，然后每一次递减1，最后到0停止，CountDownLatch的使用时一次性的，计数为0时，不可重复使用
                     * 等递减到0时，执行该代码的线程并不会停止，
                     */
                    LATCH.countDown();
                    System.out.println("i=" + i + ", latch=" + LATCH.getCount());
                    Tools.randomSleep(1000, false);
                }
            }
        }, "t");
        t.start();
        // 执行该代码的线程（main）睡眠等待，这里有个问题：如果由于代码逻辑问题，CountDownLatch一直到不了0，那么main线程会一直睡眠
        // LATCH.await();
        // 可以设置超时时间来解决永远到不了0的情况
        LATCH.await(1, TimeUnit.MINUTES);
        System.out.println("it's done, data=" + data);
    }
}
