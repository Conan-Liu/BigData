package com.conan.bigdata.common.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * 1. 实现Callable接口，通过FutureTask来获取线程返回值
 * 2. 线程池提交程序，返回Future
 */
public class CallableExecutor implements Callable<String> {
    private int id;

    public CallableExecutor(int id) {
        this.id = id;
    }

    /**
     * 线程池调用例子中, for循环启动了10次任务, 但是线程可以重复利用, 所以, 线程数可以不是10个
     *
     * @return
     * @throws Exception
     */
    @Override
    public String call() throws Exception {
        System.out.println("call() 方法被自动调用！！！" + Thread.currentThread().getName());

        // 该返回结果将被 Future 的get方法得到
        return "call() 方法被自动调用， 任务结果返回: " + id + " = " + Thread.currentThread().getName();
    }

    public static void main(String[] args) throws Exception {
        // FutureTask 获取线程返回值
        FutureTask<String> task = new FutureTask<String>(new CallableExecutor(1000));
        new Thread(task).start();
        while (!task.isDone()) ;
        System.out.println(task.get());

        // 线程池 获取线程返回值
        ExecutorService executorService = Executors.newCachedThreadPool();
        List<Future<String>> resultList = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            Future<String> future = executorService.submit(new CallableExecutor(i));
            resultList.add(future);
        }

        for (Future<String> future : resultList) {
            try {
                while (!future.isDone()) ;
                System.out.println(future.get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                executorService.shutdown();
            }
        }
    }
}