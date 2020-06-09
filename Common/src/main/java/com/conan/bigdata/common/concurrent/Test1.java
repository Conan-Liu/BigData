package com.conan.bigdata.common.concurrent;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Test1 {

    final static Lock lock = new ReentrantLock();

    private static final AtomicInteger COUNT = new AtomicInteger(0);

    public static void main(String[] args) throws InterruptedException {

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    synchronized (COUNT) {
                        for (int i = 0; i <= 100; i++) {
                            COUNT.set(COUNT.get() + i);
                            if (COUNT.get() > 100) {
                                COUNT.notify();
                                COUNT.wait();
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, "t");

        t.start();


        Thread tt = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    synchronized (COUNT) {
                        if (COUNT.get() <= 100) {
                            COUNT.wait();
                        }
                        System.out.println(COUNT);
                    }
                } catch (Exception e) {

                }
            }
        }, "tt");
        tt.start();
    }

}
