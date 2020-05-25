package com.conan.bigdata.common.concurrent;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Test1 implements Runnable {

    final static Lock lock = new ReentrantLock();

    private static int sum = 0;
    private static boolean isTrue = false;

    public static void main(String[] args) throws InterruptedException {

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 100; i++) {
                    sum++;
                    if (sum >= 100) {
                         isTrue = true;
                        System.out.println("a - " + sum);
                        break;
                    }
                }
            }
        }, "tttttt");
        t.start();

        // 两种方法演示返回值
         while (!isTrue) ;
//         t.join();
        System.out.println("b - " + sum);
    }

    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName());
    }
}
