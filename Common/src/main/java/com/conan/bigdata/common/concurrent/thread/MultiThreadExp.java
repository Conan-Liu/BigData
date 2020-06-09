package com.conan.bigdata.common.concurrent.thread;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 多线程环境下的一些小例子
 */
public class MultiThreadExp {

    /**
     * 两个线程交替打印信息，类似生产者消费者
     */
    public void alternatePrint(){
        final AtomicBoolean isTrue=new AtomicBoolean(true);
        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    // 注意一定要对同一个对象加锁，而且wait和notify也是对同一个对象操作
                    synchronized (isTrue) {
                        while (true) {
                            // true 执行
                            if (isTrue.get()) {
                                System.out.println("Thread - " + Thread.currentThread().getName());
                                isTrue.set(false);
                                // notify 并不能立即释放锁，切换到其它线程执行，会等到临界区的代码执行完才切换上下文，所以一般代码靠后
                                isTrue.notify();
                                isTrue.wait();
                            }
                            Thread.sleep(1000);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, "t1");


        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    synchronized (isTrue) {
                        while (true) {
                            // false 执行
                            if (!isTrue.get()) {
                                System.out.println("Thread - " + Thread.currentThread().getName());
                                isTrue.set(true);
                                isTrue.notify();
                                isTrue.wait();
                            }
                            Thread.sleep(1000);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, "t2");

        t1.start();
        t2.start();
    }


    /**
     * 获取另外一个线程的值
     */
    public void show2(){
        final AtomicInteger COUNT=new AtomicInteger(0);
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
                                break;
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
                            } else {
                                System.out.println(COUNT);
                                COUNT.notify();
                            }
                    }
                } catch (Exception e) {

                }
            }
        }, "tt");
        tt.start();
    }


    /**
     * 死锁演示
     * {@link com.conan.bigdata.common.concurrent.DeadLock}
     */


    public static void main(String[] args) {
        MultiThreadExp exp=new MultiThreadExp();
        exp.show2();
    }
}
