package com.conan.bigdata.common.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by Administrator on 2019/1/25.
 */
public class CallableExecutor implements Callable<String> {
    private int id;

    public CallableExecutor(int id) {
        this.id = id;
    }

    /**
     * 会启动部分线程来执行这个任务,  for循环启动了10次任务, 但是线程可以重复利用, 所以, 线程数可以不是10个
     * @return
     * @throws Exception
     */
    @Override
    public String call() throws Exception {
        System.out.println("call() 方法被自动调用！！！" + Thread.currentThread().getName());

        // 该返回结果将被 Future 的get方法得到
        return "call()方法被自动调用， 任务结果返回: " + id + " = " + Thread.currentThread().getName();
    }

    public static void main(String[] args) {
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
            } catch (ExecutionException e) {
                e.printStackTrace();
            } finally {
                executorService.shutdown();
            }
        }
    }
}