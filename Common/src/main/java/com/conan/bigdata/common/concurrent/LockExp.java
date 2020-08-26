package com.conan.bigdata.common.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * {@link Lock}显示锁演示，具体的实现类{@link ReentrantLock}
 * 公平锁：获取不到锁的时候，会自动加入队列，等待线程释放后，队列的第一个线程获取锁，也就是排队的概念，遵循先来后到
 * 非公平锁：获取不到锁的时候，会自动加入队列，等待线程释放锁后所有等待的线程同时去竞争，没有排队的概念，大家互相争抢，没有先来后到
 */
public class LockExp {

    private static final Logger log = LoggerFactory.getLogger("LockExp");

    private static class LockRunnable implements Runnable {

        private Lock lock;

        LockRunnable(Lock lock) {
            this.lock = lock;
        }

        @Override
        public void run() {
            lock.lock();
            try {
                System.out.println(Thread.currentThread().getName() + " 获取锁");
            } finally {
                lock.unlock();
            }
        }
    }

    private static void test(boolean isFair) {
        Lock lock = new ReentrantLock(isFair);
        Thread[] tArr = new Thread[10];
        for (int i = 0; i < 10; i++) {
            tArr[i] = new Thread(new LockRunnable(lock), "t - " + i);
        }
        for (int i = 0; i < 10; i++) {
            tArr[i].start();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        // 非公平锁演示
        test(false);

        TimeUnit.SECONDS.sleep(3);
        log.info("----------------------------------");

        // 公平锁演示
        test(true);
    }
}
