package com.conan.bigdata.common.concurrent.thread;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 多线程顺序打印的经典问题
 */
public class SortedPrintExp {

    public static void main(String[] args) throws InterruptedException {
        // printABCMethod();
        printABCConditionMethod();
    }


    /**
     * 开启三个线程，三个线程ID分别为A，B，C
     * 每个线程将自己的ID在屏幕上打印10遍，要求输出结果显示为
     * ABCABCA ...
     * Wait方法，增加了过早唤醒的线程开销
     */

    private static final AtomicInteger count = new AtomicInteger(0);
    private static final int nThread = 4;

    private static void printABCMethod() throws InterruptedException {
        Thread t;
        for (int i = 0; i < nThread; i++) {
            t = new Thread(new PrintABC(i, (char) (i + 65) + " "));
            t.start();
        }
    }

    private static class PrintABC implements Runnable {

        // 定义线程ID，该线程ID，只是代码中定义的ID，不是运行时操作系统分配的ID，线程Name同理
        private Integer threadId;
        private String threadName;

        private PrintABC(Integer threadId, String threadName) {
            this.threadId = threadId;
            this.threadName = threadName;
        }

        @Override
        public void run() {
            try {
                synchronized (count) {
                    for (int i = 0; i < 10; i++) {
                        // 注意这里需要循环判断，避免线程过早唤醒后，条件还不满足，但是这样不满足条件的线程又重新wait，增加了线程上下文切换的资源消耗
                        while (count.get() % nThread != this.threadId) {
                            count.wait();
                        }
                        System.out.print(this.threadName);
                        count.incrementAndGet();
                        count.notifyAll();
                        // 这里需要一个判断，否则最后一个线程无法退出
                        if (i < 9)
                            count.wait();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * {@link java.util.concurrent.locks.Condition}避免过早唤醒，和锁配合使用做到唤醒指定线程
     */
    private static final Lock LOCK = new ReentrantLock();
    private static final Condition aCondition = LOCK.newCondition();
    private static final Condition bCondition = LOCK.newCondition();
    private static final Condition cCondition = LOCK.newCondition();

    private static void printABCConditionMethod() throws InterruptedException {
        Thread t1 = new Thread(new PrintACondition(0, "A"), "A");
        Thread t2 = new Thread(new PrintBCondition(1, "B"), "B");
        Thread t3 = new Thread(new PrintCCondition(2, "C"), "C");
        t2.start();
        t3.start();
        // 唤醒必须限制顺序，否则可能造成A先唤醒B，这个时候B还没运行到await方法，然后B执行到该方法后，就永远无法唤醒了
        Thread.sleep(1000);
        t1.start();
    }


    private static class PrintACondition implements Runnable {

        // 定义线程ID，该线程ID，只是代码中定义的ID，不是运行时操作系统分配的ID，线程Name同理
        private Integer threadId;
        private String threadName;

        private PrintACondition(Integer threadId, String threadName) {
            this.threadId = threadId;
            this.threadName = threadName;
        }

        @Override
        public void run() {
            LOCK.lock();
            try {
                for (int i = 0; i < 10; i++) {
                    System.out.print(threadName);
                    bCondition.signal();
                    aCondition.await();
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                LOCK.unlock();
            }
        }
    }

    private static class PrintBCondition implements Runnable {

        // 定义线程ID，该线程ID，只是代码中定义的ID，不是运行时操作系统分配的ID，线程Name同理
        private Integer threadId;
        private String threadName;

        private PrintBCondition(Integer threadId, String threadName) {
            this.threadId = threadId;
            this.threadName = threadName;
        }

        @Override
        public void run() {
            LOCK.lock();
            try {
                for (int i = 0; i < 10; i++) {
                    bCondition.await();
                    System.out.print(threadName);
                    cCondition.signal();
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                LOCK.unlock();
            }
        }
    }

    private static class PrintCCondition implements Runnable {

        // 定义线程ID，该线程ID，只是代码中定义的ID，不是运行时操作系统分配的ID，线程Name同理
        private Integer threadId;
        private String threadName;

        private PrintCCondition(Integer threadId, String threadName) {
            this.threadId = threadId;
            this.threadName = threadName;
        }

        @Override
        public void run() {
            LOCK.lock();
            try {
                for (int i = 0; i < 10; i++) {
                    cCondition.await();
                    System.out.print(threadName);
                    aCondition.signal();
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                LOCK.unlock();
            }
        }
    }

}
