package com.conan.bigdata.common.concurrent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 */
public class RunnableExecutor implements Runnable {

    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName() + " 线程被调用...");
    }

    /**
     * 虽然执行了 5次循环, 但是线程却不一定是5个, 随机执行, 可能新建一个线程, 也可能是已经创建好的线程
     * 这是线程池的特点, 重复利用线程, 减少了线程创建销毁的资源浪费
     * @param args
     */
    public static void main(String[] args) {
        ExecutorService executorService = Executors.newCachedThreadPool();
        for (int i = 0; i < 10; i++) {
            // 无 返回值线程
            executorService.execute(new RunnableExecutor());
            // 有 返回值线程
            // Future<T> future=executorService.submit(new RunnableExecutor(), T)
            System.out.println("******** a" + i + "**********");
        }
        executorService.shutdown();
    }
}