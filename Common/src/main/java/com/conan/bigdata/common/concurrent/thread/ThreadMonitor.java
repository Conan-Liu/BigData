package com.conan.bigdata.common.concurrent.thread;

import com.conan.bigdata.common.util.Tools;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 该类功能
 * 启动一个工作线程WorkerThread，模拟执行任务，该线程通过setUncaughtExceptionHandler指定一个未捕获的异常处理线程，等于有两个线程
 * WorkerThread工作线程模拟随机抛出异常
 * ThreadMonitorInner异常处理线程实现，记录异常信息，并负责重启工作线程
 */
public class ThreadMonitor {

    private volatile boolean inited = false;
    private static int threadIndex = 0;
    private final BlockingQueue<String> channel = new LinkedBlockingQueue<>(100);

    public static void main(String[] args) throws InterruptedException {
        ThreadMonitor demo = new ThreadMonitor();
        for (int i = 0; i < 100; i++) {
            demo.channel.put("message - " + i);
        }

        demo.init();
    }

    private synchronized void init() {
        if (!inited) {
            System.out.println("init...");
            Thread t = new Thread(new WorkerThread(), "monitor-" + threadIndex++);
            t.setUncaughtExceptionHandler(new ThreadMonitorInner());
            t.start();
            inited = true;
        }
    }

    private class WorkerThread implements Runnable {

        private final Random random = new Random();

        @Override
        public void run() {
            System.out.println("worker thread do some thing...");
            String msg;
            try {
                while (true) {
                    msg = channel.take();
                    process(msg);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        private void process(String message) {
            System.out.println("模拟随机异常: " + message);
            int i = random.nextInt(100);
            System.out.println("i = " + i);
            if (i < 40) {
                throw new RuntimeException("抛出异常");
            } else {
                System.out.println("正常...");
            }
            Tools.randomSleep(10000, false);
        }
    }

    /**
     * Thread类内部接口 UncaughtExceptionHandler
     * 该线程可以在调用的线程异常后，启动执行，主要功能可以记录异常信息，然后重新启动一个新的原来线程继续执行任务
     */
    private class ThreadMonitorInner implements Thread.UncaughtExceptionHandler {

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            System.out.println(t.getName() + ", is still alive:" + t.isAlive());

            // 异常信息处理
            System.out.println(e);

            // 创建并启动替代线程
            System.out.println("重新启动线程 - " + t.getName());
            inited = false;
            init();
        }
    }
}
