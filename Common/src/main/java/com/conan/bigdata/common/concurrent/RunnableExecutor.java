package com.conan.bigdata.common.concurrent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Administrator on 2019/1/25.
 */
public class RunnableExecutor implements Runnable {

    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName() + " 线程被调用...");
    }

    /**
     * 虽然执行了 5次循环， 但是线程却不一定是5个，  随机执行， 可能新建一个线程， 也可能是已经创建好的线程
     * @param args
     */
    public static void main(String[] args) {
        ExecutorService executorService = Executors.newCachedThreadPool();
        for (int i = 0; i < 5; i++) {
            executorService.execute(new RunnableExecutor());
            System.out.println("******** a" + i + "**********");
        }
        executorService.shutdown();
    }
}