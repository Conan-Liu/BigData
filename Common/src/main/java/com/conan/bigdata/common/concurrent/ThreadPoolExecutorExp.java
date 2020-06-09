package com.conan.bigdata.common.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * 1. 实现Callable接口，通过FutureTask来获取线程返回值
 * 2. 线程池提交程序，返回Future
 * 3. 线程池的名称默认是 pool-PoolNum-thread-X，这是由默认的ThreadFactory类来控制的{@link Executors.DefaultThreadFactory}，如果想改变名称，可以自定义ThreadFactory
 */
public class ThreadPoolExecutorExp extends ThreadPoolExecutor {


    public ThreadPoolExecutorExp(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }

    public ThreadPoolExecutorExp(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    }

    public ThreadPoolExecutorExp(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
    }

    public ThreadPoolExecutorExp(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
    }

    /**
     * ThreadPoolExecutor 提供两个钩子方法，在线程执行前和执行完成后（不管是否异常）执行
     * Thread t 表示要执行任务的线程
     * Runable r 表示需要执行的任务，这是个Runable实例
     */
    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        System.out.println(t.getName() + "开始执行，实例" + r);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        System.out.println("线程结束...");
    }

    private static class CallableExp implements Callable<String> {
        private int id;

        public CallableExp(int id) {
            this.id = id;
        }

        /**
         * 线程池调用例子中, for循环启动了10次任务, 但是线程可以重复利用, 所以, 线程数可以不是10个
         * 该方法和Runable接口的run方法一样，是由JVM自动调用
         */
        @Override
        public String call() throws Exception {
            System.out.println("call() 方法被自动调用！！！" + Thread.currentThread().getName());

            // 该返回结果将被 Future 的get方法得到
            return "call() 方法被自动调用， 任务结果返回: " + this.id + " = " + Thread.currentThread().getName();
        }
    }

    private static class RunableExp implements Runnable {

        @Override
        public void run() {
            System.out.println("Runable");
        }
    }

    public static void main(String[] args) throws Exception {
        // FutureTask 获取线程返回值
        FutureTask<String> task = new FutureTask<String>(new CallableExp(1000));
        new Thread(task, "Call-1000").start();
        // 注意这里主线程无法知道子线程执行情况，是通过轮询的方式来获取子线程的执行状态，任务执行完成，异常，取消都返回true
        // while (!task.isDone()) ;
        // get方法是阻塞式的，可以不用isDone判断
        System.out.println("1: " + task.get());

        // 线程池 获取线程返回值
        ExecutorService executorService = Executors.newCachedThreadPool();
        List<Future<String>> resultList = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            Future<String> future = executorService.submit(new CallableExp(i));
            resultList.add(future);
        }

        // ThreadPoolExecutor 提供一些方法来监控线程池
        System.out.println("活跃线程数1:" + ((ThreadPoolExecutor) executorService).getActiveCount());
        for (Future<String> future : resultList) {
            try {
                // Future.get()是阻塞式方法
                // while (!future.isDone()) ;
                System.out.println(future.get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                executorService.shutdown();
            }
        }


        // 验证ThreadPoolExecutor的两个钩子方法
        ThreadPoolExecutorExp exp = new ThreadPoolExecutorExp(3, 6, 4, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(16));
        exp.submit(new RunableExp());
        exp.shutdown();
    }
}