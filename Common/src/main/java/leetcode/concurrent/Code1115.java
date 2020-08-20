package leetcode.concurrent;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Code1115 {

    private static final int n = 5;
    private static AtomicBoolean isFoo = new AtomicBoolean(true);

    // 可以使用循环判断的方式来输出，这个最少需要使用2core
    private static class Foo implements Runnable {

        @Override
        public void run() {
            for (int i = 0; i < n; i++) {
                while (!isFoo.get()) {

                }
                System.out.print("foo");
                isFoo.getAndSet(false);
            }
        }
    }

    private static class Bar implements Runnable {

        @Override
        public void run() {
            for (int i = 0; i < n; i++) {
                while (isFoo.get()) {

                }
                System.out.print("bar");
                isFoo.getAndSet(true);
            }
        }
    }

    private static final Lock lock = new ReentrantLock();
    private static final Condition condition1 = lock.newCondition();
    private static final Condition condition2 = lock.newCondition();

    private static class Foo1 implements Runnable {

        @Override
        public void run() {
            lock.lock();
            try {
                for (int i = 0; i < n; i++) {
                    if (!isFoo.get()) {
                        condition1.await();
                    }
                    System.out.print("foo");
                    isFoo.getAndSet(false);
                    condition2.signal();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                lock.unlock();
            }
        }
    }

    private static class Bar1 implements Runnable {

        @Override
        public void run() {
            lock.lock();
            try {
                for (int i = 0; i < n; i++) {
                    if (isFoo.get()) {
                        condition2.await();
                    }
                    System.out.print("bar");
                    isFoo.getAndSet(true);
                    condition1.signal();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                lock.unlock();
            }
        }
    }

    public static void main(String[] args) {
        Thread t1 = new Thread(new Foo1());
        Thread t2 = new Thread(new Bar1());
        t1.start();
        t2.start();
    }
}
